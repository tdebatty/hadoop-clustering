/* To run :
 * /path/to/bin/hadoop jar 
 *     /path/to/hadoop-clustering.jar
 *     gmeans.Main 
 *     -libjars /home/tibo/Java/spymemcached-2.9.1.jar,/home/tibo/Java/commons-math3-3.2/commons-math3-3.2.jar
 *     /dataset_400_k4_d3.csv
 */


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
    public int kmeans_iterations = 5;
    
    protected Configuration conf;
    protected int gmeans_iteration = 0;

    Gmeans(Configuration conf) {
        this.conf = conf;
    }
    
    public int run() {
       /* try {
            //WriteInitialCenterToCache();
            
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
            return 1;
        }*/

        while (true) {
            gmeans_iteration++;
            
            try {
                Find2Centers();
                Perform2Means();
                TestNewCenters();
                
                if (ClusteringCompleted()) {
                    System.out.println("Clustering completed!! :-)");
                    return 0;
                }
                
                WritePoints();
                
            } catch (IOException ex) {
                Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
                return 1;
            }

            if (gmeans_iteration == max_iterations) {
                System.out.println("Max iterations count reached...");
                break;
            }
        }
        
        return 0;
    }
    
    /**
     * Write first point of dataset to cache
     * in IT-0_TEST_0
     * @throws IOException 
     */
    protected void WriteInitialCenterToCache() throws IOException {
        JobConf job = new JobConf(conf);

        FileSystem fs = FileSystem.get(job);
        InputStream in = fs.open(new Path(input_path));
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        Point point = Point.parse(br.readLine());
        memcached.set("IT-0_TEST_0", 0, point.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }
    
    protected void Find2Centers() throws IOException {
        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : Find2Centers");
        System.out.println("Gmeans : " + gmeans_iteration + " : Find2Centers");
        System.out.println("---------------------------------");

        String job_input = this.output_path + "it" + (gmeans_iteration - 1);
        
        if (gmeans_iteration == 1) {
            job_input = this.input_path;
        }
        
        FileInputFormat.setInputPaths(job, new Path(job_input));
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(Find2CentersMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // center id
        job.setMapOutputValueClass(Point.class);

        //job.setCombinerClass(XXX.class);

        job.setReducerClass(Find2CentersReducer.class);
        // Nothing to write : centers will go to cache
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("gmeans_iteration", gmeans_iteration);

        JobClient.runJob(job);
    }

    protected void Perform2Means() throws IOException {
        for (int kmeans_iteration = 1; kmeans_iteration <= kmeans_iterations; kmeans_iteration++) {
            
            // Create a JobConf using the conf processed by ToolRunner
            JobConf job = new JobConf(conf, getClass());
            job.setJobName("Gmeans : " + gmeans_iteration + " : 2Means : " + kmeans_iteration);
            System.out.println("Gmeans : " + gmeans_iteration + " : 2Means : " + kmeans_iteration);    
            System.out.println("---------------------------------");

            String job_input = this.output_path + "it" + (gmeans_iteration - 1);

            if (gmeans_iteration == 1) {
                job_input = this.input_path;
            }

            FileInputFormat.setInputPaths(job, new Path(job_input));
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(Perform2MeansMapper.class);
            job.setMapOutputKeyClass(LongWritable.class); // center id
            job.setMapOutputValueClass(Point.class);

            //job.setCombinerClass(XXX.class);

            job.setReducerClass(Perform2MeansReducer.class);
            // Nothing to write : centers will go to distributed cache
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormat(NullOutputFormat.class);

            job.setInt("gmeans_iteration", gmeans_iteration);
            job.setInt("kmeans_iteration", kmeans_iteration);

            JobClient.runJob(job);
        }
    }
    
    protected void TestNewCenters() throws IOException {
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : Test");
        System.out.println("Gmeans : " + gmeans_iteration + " : Test");
        System.out.println("---------------------------------");

        String job_input = this.output_path + "it" + (gmeans_iteration - 1);
        
        if (gmeans_iteration == 1) {
            job_input = this.input_path;
        }
        
        FileInputFormat.setInputPaths(job, new Path(job_input));
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // center id
        job.setMapOutputValueClass(DoubleWritable.class);

        //job.setCombinerClass(XXX.class);

        job.setReducerClass(TestReducer.class);
        // Nothing to write : centers will go to distributed cache
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("gmeans_iteration", gmeans_iteration);
        job.setInt("kmeans_iterations", kmeans_iterations);

        JobClient.runJob(job);

    }

    private void WritePoints() throws IOException {
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + gmeans_iteration + " : WritePoints");
        System.out.println("Gmeans : " + gmeans_iteration + " : WritePoints");
        System.out.println("---------------------------------");

        String job_input = this.output_path + "it" + (gmeans_iteration - 1);
        
        if (gmeans_iteration == 1) {
            job_input = this.input_path;
        }
        
        FileInputFormat.setInputPaths(job, new Path(job_input));
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(WritePointsMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // center id
        job.setMapOutputValueClass(Point.class);

        //job.setCombinerClass(XXX.class);

        // Using IdentityReducer, Points will be shuffled (sorted) by center_id
        // this can improve following iterations, if a combiner is used...
        job.setReducerClass(IdentityReducerWithoutKey.class);
        //job.setOutputKeyClass(NullWritable.class);
        //job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(this.output_path + "it" + gmeans_iteration));

        job.setInt("gmeans_iteration", gmeans_iteration);
        job.setInt("kmeans_iterations", kmeans_iterations);
        
        JobClient.runJob(job);

    }

    private boolean ClusteringCompleted() throws IOException {
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
    }
}