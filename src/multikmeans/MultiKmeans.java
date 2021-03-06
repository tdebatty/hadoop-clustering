package multikmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import kmeans.Point;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/**
 *
 * @author tibo
 */
public class MultiKmeans {
    public int iterations = 5;
    public int k_min = 1;
    public int k_max = 10;
    public int k_step = 1;
    public String input_path = "";
    public int num_reduce_tasks = 2;
    public String memcached_server = "127.0.0.1";
    
    protected Configuration conf;
    protected int iteration;

    MultiKmeans(Configuration conf) {
        this.conf = conf;
    }
    
    public int run() {
        System.out.println("MultiKmeans clustering");
        System.out.println("Iterations: " + iterations);
        System.out.println("Input path: " + input_path);
        
        long start = System.currentTimeMillis();
        long end;
        
        try {
            PickInitialCenters();
        } catch (IOException ex) {
            Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
            return 1;
        }
        
        
        for (iteration=0; iteration < iterations; iteration++) {

            // Create a JobConf using the conf processed by ToolRunner
            JobConf job = new JobConf(conf, getClass());
            job.setJobName("MultiKmeans : " + iteration);
            System.out.println("MultiKmeans : " + iteration);
            
            FileInputFormat.setInputPaths(job, new Path(input_path));
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(MultiKmeansMapper.class);
            job.setMapOutputKeyClass(Text.class); // k_centerid
            job.setMapOutputValueClass(Point.class);
            
            job.setCombinerClass(MultiKmeansCombiner.class);
            
            job.setReducerClass(MultiKmeansReducer.class);
            // Nothing to write : centers will go to cache
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormat(NullOutputFormat.class);
            
            job.setNumReduceTasks(num_reduce_tasks);
                
            job.setInt("k_min", k_min);
            job.setInt("k_max", k_max);
            job.setInt("k_step", k_step);
            job.setInt("iteration", iteration);
            job.set("memcached_server", memcached_server);
            
            try {
                JobClient.runJob(job);
            } catch (IOException ex) {
                Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
                return 1;
            }
            
            end = System.currentTimeMillis();
            System.out.println("Execution time (so far): " + (end - start) + " ms");
        }
        
        
        
        System.out.println("Clustering completed!");
        end = System.currentTimeMillis();
        System.out.println("Execution time: " + (end - start) + " ms");
        return 0;
    }
    
    private void PickInitialCenters() throws IOException {
        JobConf job = new JobConf(conf);

        FileSystem fs = FileSystem.get(job);
        FileStatus fstatus = fs.getFileStatus(new Path(input_path));
        
        String input_file = input_path;
        if (fstatus.isDir()) {
            // TODO : fetch first file
            input_file = input_path + "/part-00000";
        }
        InputStream in = fs.open(new Path(input_file));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        MemcachedClient memcached = new MemcachedClient(AddrUtil.getAddresses(memcached_server));
 
        Point point = new Point();
        
        for (int i = 0; i < k_max; i++) {
            point.parse(br.readLine());
            for (int k = k_min; k <= k_max; k += k_step) {
                //System.out.println(point);
                if (i < k) {
                    //System.out.println("Write init center 0_" + k + "_" + i);
                    memcached.set("0_" + k + "_" + i, 0, point.toString());
                }
            }
        }

        memcached.shutdown(5, TimeUnit.SECONDS);
    }
}
