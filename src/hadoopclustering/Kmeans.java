/* To run :
 * /opt/hadoop-1.2.1/bin/hadoop jar 
 *     ./dist/hadoop-clustering.jar
 *     hadoopclustering.Kmeans
 *     -libjars /home/tibo/Java/spymemcached-2.9.1.jar 
 *     /synthetic.data
 * 
 * */
package hadoopclustering;


import java.io.*;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Kmeans extends Configured implements Tool {
    public int iterations = 3;
    public int k = 10;
    public String input_path = "";
    
    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new Kmeans(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) {
        // Parse arguments
        input_path = args[0];
        
        try {
            writeInitialCentersToCache();
        } catch (IOException ex) {
            Logger.getLogger(Kmeans.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        for (int i=0; i < iterations; i++) {

            // Create a JobConf using the conf processed by ToolRunner
            JobConf job = new JobConf(getConf(), getClass());
            job.setJobName("my-app");
            
            FileInputFormat.setInputPaths(job, new Path(input_path));
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(KmeansMap.class);
            job.setMapOutputKeyClass(LongWritable.class); // center id
            job.setMapOutputValueClass(Point.class);
            
            job.setCombinerClass(KmeansCombine.class);
            
            job.setReducerClass(KmeansReduce.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormat(NullOutputFormat.class);
            // Nothing to write : centers will go to distributed cache
            
            job.setInt("iteration", i);
            job.setInt("k", k);
            
            try {
                JobClient.runJob(job);
            } catch (IOException ex) {
                Logger.getLogger(Kmeans.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }
    
    protected void writeInitialCentersToCache() throws IOException {
        JobConf job = new JobConf(getConf());

        FileSystem fs = FileSystem.get(job);
        InputStream in = fs.open(new Path(input_path));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        MemcachedClient memcached = new MemcachedClient(
                    new InetSocketAddress("127.0.0.1", 11211));
 
        for (int i = 0; i < this.k; i++) {
            Point point = Point.parse(br.readLine());
            //System.out.println(point);
            memcached.set("center_0_" + i, 0, point.toString()); // Point implements Serializable
        }

        memcached.shutdown(5, TimeUnit.SECONDS);
    }
}
