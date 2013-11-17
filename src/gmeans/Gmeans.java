package gmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    public String input_path = "";
    public String output_path = "/gmeans/";
    public int max_iterations = 10;
    
    protected Configuration conf;
    protected int gmeans_iteration = 0;

    Gmeans(Configuration conf) {
        this.conf = conf;
    }
    
    public int run() {
        System.out.println("Gmeans clustering");
        System.out.println("Input path: " + input_path);
        System.out.println("Output path:" + output_path);
        
        long start = System.currentTimeMillis();
        
        try {
            Write2CentersToCache();
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
            return 1;
        }
        
        while (true) {
            gmeans_iteration++;
            
            try {
                Perform2Means();
                Perform2Means();
                Perform2Means();
                Perform2MeansAndFind2Centers();
                TestNewCenters();
                
                if (ClusteringCompleted()) {
                    break;
                }
                
            } catch (IOException ex) {
                Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
                return 1;
            }

            if (gmeans_iteration == max_iterations) {
                System.out.println("Max iterations count reached...");
                return 1;
            }
        }
        
        long end = System.currentTimeMillis();
        System.out.println("Clustering completed!! :-)");
        System.out.println("Execution time: " + (end - start) + " ms");
        return 0;
    }
    
    private void Write2CentersToCache() throws IOException {
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

        MemcachedClient memcached = new MemcachedClient(
                    new InetSocketAddress("127.0.0.1", 11211));
 
        for (int i = 0; i < 2; i++) {
            Point point = Point.parse(br.readLine());
            //System.out.println(point);
            memcached.set("IT-1_CENTER-" + i, 0, point.toString());
        }

        memcached.shutdown(5, TimeUnit.SECONDS);
    }
    
    private void Perform2MeansAndFind2Centers() throws IOException {
        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : 2Means & Find2Centers");
        System.out.println("Gmeans : " + gmeans_iteration + " : 2Means & Find2Centers");

        FileInputFormat.setInputPaths(job, new Path(this.input_path));
        job.setInputFormat(TextInputFormat.class);

        /* MAPPER
         * input: Text
         * output: center_id => coordinates, 1 (Point implements writable)
         * classical k-means mapper : assign point to most close center
         */
        job.setMapperClass(Perform2MeansAndFind2CentersMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Point.class);

        /* COMBINER
         * input: center_id => <coordinates, 1>
         * output:
         * center_id => coordinates, count     // partial center for k-means
         * center_id + OFFSET : coordinates, 1 // coordinates of first point in this cluster
         * center_id + OFFSET : coordinates, 1 // coordinates of second point in this cluster
         */
        //job.setCombinerClass(Perform2MeansAndFind2CentersCombiner.class);

        /* REDUCER
         * input: center_id, <coordinates, count>
         * output: nothing, centers will go to distributed cache
         * 
         * if center_id < OFFSET : reduce center and write to cache
         * else : write 2 new centers to cache for next iteration
         * 
         */
        job.setReducerClass(Perform2MeansAndFind2CentersReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("gmeans_iteration", gmeans_iteration);

        JobClient.runJob(job);
    }
     
    private void TestNewCenters() throws IOException {
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : Test");
        System.out.println("Gmeans : " + gmeans_iteration + " : Test");
        
        FileInputFormat.setInputPaths(job, new Path(this.input_path));
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // center id
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("gmeans_iteration", gmeans_iteration);

        JobClient.runJob(job);

    }

    private boolean ClusteringCompleted() throws IOException {
        return false;
        /*
        int max_centers = (int) Math.pow(2, gmeans_iteration);
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        String prefix = "IT-" + gmeans_iteration + "_TEST_";
        for (int i = 0; i<max_centers; i++) {
            String key = prefix + i;
            if (memcached.get(key) != null) {
                memcached.shutdown();
                return false;
            }
        }
        
        memcached.shutdown();
        return true;
        */
    }

    private void Perform2Means() throws IOException {
        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : 2Means");
        System.out.println("Gmeans : " + gmeans_iteration + " : 2Means");

        FileInputFormat.setInputPaths(job, new Path(this.input_path));
        job.setInputFormat(TextInputFormat.class);

        /* MAPPER
         * input: Text
         * output: center_id => coordinates, 1 (Point implements writable)
         * classical k-means mapper : assign point to most close center
         */
        job.setMapperClass(Perform2MeansMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Point.class);

        /* COMBINER
         * input: center_id => <coordinates, 1>
         * output:
         * center_id => coordinates, count     // partial center for k-means
         * center_id + OFFSET : coordinates, 1 // coordinates of first point in this cluster
         * center_id + OFFSET : coordinates, 1 // coordinates of second point in this cluster
         */
        //job.setCombinerClass(Perform2MeansAndFind2CentersCombiner.class);

        /* REDUCER
         * input: center_id, <coordinates, count>
         * output: nothing, centers will go to distributed cache
         * 
         * if center_id < OFFSET : reduce center and write to cache
         * else : write 2 new centers to cache for next iteration
         * 
         */
        job.setReducerClass(Perform2MeansReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("gmeans_iteration", gmeans_iteration);

        JobClient.runJob(job);
    }
}