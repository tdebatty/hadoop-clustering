package kmeans;


import java.io.*;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/**
 *
 * @author tibo
 */
public class Kmeans  {
    public int iterations = 3;
    public int k = 10;
    public String input_path = "";
    
    protected Configuration conf;

    Kmeans(Configuration conf) {
        this.conf = conf;
    }
    
    public int run() {
        try {
            writeInitialCentersToCache();
        } catch (IOException ex) {
            Logger.getLogger(Kmeans.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        for (int i=0; i < iterations; i++) {

            // Create a JobConf using the conf processed by ToolRunner
            JobConf job = new JobConf(conf, getClass());
            job.setJobName("Kmeans");
            
            FileInputFormat.setInputPaths(job, new Path(input_path));
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(KmeansMap.class);
            job.setMapOutputKeyClass(LongWritable.class); // center id
            job.setMapOutputValueClass(Point.class);
            
            job.setCombinerClass(KmeansCombine.class);
            
            job.setReducerClass(KmeansReduce.class);
            // Nothing to write : centers will go to distributed cache
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormat(NullOutputFormat.class);
                
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
        JobConf job = new JobConf(conf);

        FileSystem fs = FileSystem.get(job);
        InputStream in = fs.open(new Path(input_path));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        MemcachedClient memcached = new MemcachedClient(
                    new InetSocketAddress("127.0.0.1", 11211));
 
        for (int i = 0; i < this.k; i++) {
            Point point = Point.parse(br.readLine());
            //System.out.println(point);
            memcached.set("center_0_" + i, 0, point.toString());
        }

        memcached.shutdown(5, TimeUnit.SECONDS);
    }
}