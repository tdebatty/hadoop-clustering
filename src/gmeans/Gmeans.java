package gmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;

/**
 *
 * @author tibo
 */
public class Gmeans  {
    public static final String CENTER_KEY_FORMAT = "IT-%d_CENTER-%d";
    
    
    public String input_path = "";
    public int max_iterations = 10;
    public String memcached_servers = "127.0.0.1";
 
    public int task_heap = 1024; // in MB
    public double memory_coeff = 1.5;
    
    public int num_reduce_tasks = 2; // Should be 95% of max reduce capacity
    
    protected Configuration conf;
    protected int gmeans_iteration = 1;
    protected MemcachedClient memcached;
    protected long start;
    
    // Number of points in the biggest cluster
    // Used to choose between ''test clusters'' and ''test few clusters''
    protected int current_biggest_cluster_size;
    protected long max_cluster_size;

    Gmeans(Configuration conf) {
        this.conf = conf;
    }
    
    public int run() {
        conf.set("mapred.child.java.opts", "-Xmx" + task_heap  + "m");
        max_cluster_size = (long) Math.floor(task_heap * 1024 * 1024 / 64 /  memory_coeff);
        
        System.out.println("G-means clustering");
        System.out.println("Input path: " + input_path);
        System.out.println("Num reduce tasks: " + num_reduce_tasks);
        System.out.println("Memcached servers: " + memcached_servers);
        System.out.println("Task heap size: " + task_heap + "MB");
        System.out.println("Max points per test task: " + max_cluster_size);
        
        start = System.currentTimeMillis();
        
        try {
            memcached = new MemcachedClient(AddrUtil.getAddresses(memcached_servers));
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not connect to Memcached server!");
            return 1;
        }
        
        
        try {
            PickInitialCenters();
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
            return 1;
        }
        
        while (!ClusteringCompleted()) {
            if (gmeans_iteration > max_iterations) {
                System.out.println("Max iterations count reached...");
                return 1;
            }
        
            try {
                //KMeans();
                KMeans();
                KMeans();
                KMeansAndFindNewCenters();
                TestClusters();
                
            } catch (IOException ex) {
                Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
                return 1;
            }

            gmeans_iteration++;
        }
        
        
        gmeans_iteration--;
        System.out.println("Clustering completed after " + gmeans_iteration  + " iterations!! :-)");
        System.out.println("Total execution time: " + (System.currentTimeMillis() - start) + " ms");
        
        memcached.set("gmeans_last_iteration", 0, gmeans_iteration);
        memcached.shutdown(5, TimeUnit.SECONDS);
        return 0;
    }
    
    private void PickInitialCenters() throws IOException {
        JobConf job = new JobConf(conf);

        FileSystem fs = FileSystem.get(job);
        FileStatus fstatus = fs.getFileStatus(new Path(input_path));
        
        String input_file = input_path;
        if (fstatus.isDir()) {
            // TODO : Find first file in directory...
            input_file = input_path + "/part-00000";
        }
        InputStream in = fs.open(new Path(input_file));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

 
        for (int i = 0; i < 2; i++) {
            Point point = new Point();
            point.parse(br.readLine());
            memcached.set("IT-1_CENTER-" + i, 0, point.toString());
        }
        System.out.println("Execution time (till now): " + (System.currentTimeMillis() - start) + " ms");
    }
    
    private void KMeans() throws IOException {
        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : KMeans");
        System.out.println("Gmeans : " + gmeans_iteration + " : KMeans");

        FileInputFormat.setInputPaths(job, new Path(this.input_path));
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Point.class);

        job.setCombinerClass(KMeansCombiner.class);

        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("gmeans_iteration", gmeans_iteration);
        job.set("memcached_server", memcached_servers);
        job.setNumReduceTasks(num_reduce_tasks);

        JobClient.runJob(job);
        System.out.println("Execution time (till now): " + (System.currentTimeMillis() - start) + " ms");
    }
    
    private void KMeansAndFindNewCenters() throws IOException {
        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : KMeans & Find New Centers");
        System.out.println("Gmeans : " + gmeans_iteration + " : KMeans & Find New Centers");

        FileInputFormat.setInputPaths(job, new Path(this.input_path));
        job.setInputFormat(TextInputFormat.class);

        /* MAPPER
         * input: Text
         * output: center_id => coordinates, 1 (Point implements writable)
         * classical k-means mapper : assign point to most close center
         */
        job.setMapperClass(KMeansAndFindNewCentersMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Point.class);

        /* COMBINER
         * input: center_id => <coordinates, 1>
         * output:
         * center_id => coordinates, count     // partial center for k-means
         * center_id + OFFSET : coordinates, 1 // coordinates of first point in this cluster
         * center_id + OFFSET : coordinates, 1 // coordinates of second point in this cluster
         */
        job.setCombinerClass(KMeansAndFindNewCentersCombiner.class);

        /* REDUCER
         * input: center_id, <coordinates, count>
         * output: nothing, centers will go to distributed cache
         * 
         * if center_id < OFFSET : reduce center and write to cache
         * else : write 2 new centers to cache for next iteration
         * 
         */
        job.setReducerClass(KMeansAndFindNewCentersReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.set("memcached_server", memcached_servers);
        job.setInt("gmeans_iteration", gmeans_iteration);
        job.setNumReduceTasks(num_reduce_tasks);
        
        JobClient.runJob(job);
        System.out.println("Execution time (till now): " + (System.currentTimeMillis() - start) + " ms");
    }
     
    private void TestClusters() throws IOException {
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : Test");
        System.out.println("Gmeans : " + gmeans_iteration + " : Test");
        
        FileInputFormat.setInputPaths(job, new Path(this.input_path));
        job.setInputFormat(TextInputFormat.class);

        int num_clusters = (int) Math.pow(2, gmeans_iteration - 1);
        System.out.println("Currently " + num_clusters + " clusters to test");
        System.out.println("Biggest cluster has " + current_biggest_cluster_size + " points");
        System.out.println("Maximum number of points that can be handled by a single reducer is " + max_cluster_size);
        
        if (
                num_clusters >= num_reduce_tasks
                && current_biggest_cluster_size < max_cluster_size  ) {
            
            System.out.println("Using classical method : ADTest performed by reducer");

            job.setMapperClass(TestMapper.class);
            job.setMapOutputKeyClass(LongWritable.class); // center id
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setReducerClass(TestReducer.class);
            job.setNumReduceTasks(num_reduce_tasks);
            
            
        } else {
            System.out.println("Using alternative method : ADTest performed by mapper");
            job.setMapperClass(TestFewClustersMapper.class);
            job.setMapOutputKeyClass(LongWritable.class); // center id
            job.setMapOutputValueClass(DoubleWritable.class);
            
            job.setReducerClass(TestFewClustersReducer.class);
            job.setNumReduceTasks(num_clusters);

        }
      
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.set("memcached_server", memcached_servers);
        job.setInt("gmeans_iteration", gmeans_iteration);

        JobClient.runJob(job);
        System.out.println("Execution time (till now): " + (System.currentTimeMillis() - start) + " ms");
    }

    private boolean ClusteringCompleted() {
        if (gmeans_iteration == 1) {
            return false;
        }
        
        current_biggest_cluster_size = 0;
        
        boolean clustering_completed = true;
        int max_centers = (int) Math.pow(2, gmeans_iteration - 1);
        int found = 0;

        String prefix = "IT-" + (gmeans_iteration - 1) + "_CENTER-";
        for (int i = 0; i<max_centers; i++) {
            String key = prefix + i;
            Object value = memcached.get(key);
            if (value == null) {
                memcached.set("IT-" + (gmeans_iteration) + "_CENTER-" + i, 0, "");
                memcached.set("IT-" + (gmeans_iteration) + "_CENTER-" + (i + max_centers), 0, "");
                continue;
            }
            
            String value_s = (String) value;
            if ("".equals(value_s)) {
                // Propagate empty points
                memcached.set("IT-" + (gmeans_iteration) + "_CENTER-" + i, 0, "");
                memcached.set("IT-" + (gmeans_iteration) + "_CENTER-" + (i + max_centers), 0, "");
                continue;
            }
            
            Point point = new Point();
            point.parse(value_s);
            if (point.found) {
                found++;
                
                // Propagate this point for next iteration...
                // (it won't be overwritten by KMeans&FindNewCenters)
                memcached.set("IT-" + (gmeans_iteration) + "_CENTER-" + i, 0, point.toString());
                
                // And propagate the 2nd corresponding point (that will be left blank)
                memcached.set("IT-" + (gmeans_iteration) + "_CENTER-" + (i + max_centers), 0, "");

            } else {
                clustering_completed = false;
                
                if (point.count > current_biggest_cluster_size) {
                    current_biggest_cluster_size = (int) point.count;
                }
            }
        }
        System.out.println("Found " + found + " clusters till now...");
        return clustering_completed;
    }
}