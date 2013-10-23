package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
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
        System.out.println("Gmeans clustering");
        System.out.println("Input path: " + input_path);
        System.out.println("Output path:" + output_path);
        System.out.println("Kmeans iterations: " + kmeans_iterations);
        
        long start = System.currentTimeMillis();

        while (true) {
            gmeans_iteration++;
            
            try {
                Find2Centers();
                Perform2Means();
                TestNewCenters();
                
                if (ClusteringCompleted()) {
                    break;
                }
                
                WritePoints();
                
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