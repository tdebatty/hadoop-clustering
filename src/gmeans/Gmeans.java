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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Gmeans  {
    public String input_path = "";
    public String output_path = "/gmeans/";
    
    protected Configuration conf;
    protected int n = 0;

    Gmeans(Configuration conf) {
        this.conf = conf;
    }
    
    public int run() {
        try {
            WriteInitialCenterToCache();
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
        }

        while (true) {
            n++;
            
            Find2Centers();
            Perform2Means();
            Test();
            WriteSplit();
            
            if (n == 5) {
                break;
            }
        }
        
        return 0;
    }
    
    /**
     * Write first point of dataset to cache
     * in IT0_CENTER0_TESTED
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
        memcached.set("IT0_CENTER0_TESTED", 0, point.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }
    
    protected void Find2Centers() {
        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Gmeans : " + n + " : Find2Centers");

        String job_input = this.output_path + "it" + n;
        
        if (n == 0) {
            job_input = this.input_path;
        }
        
        FileInputFormat.setInputPaths(job, new Path(job_input));
        job.setInputFormat(TextInputFormat.class);

        job.setMapperClass(Find2CentersMapper.class);
        job.setMapOutputKeyClass(LongWritable.class); // center id
        job.setMapOutputValueClass(Point.class);

        //job.setCombinerClass(XXX.class);

        job.setReducerClass(Find2CentersReducer.class);
        // Nothing to write : centers will go to distributed cache
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormat(NullOutputFormat.class);

        job.setInt("n", n);

        try {
            JobClient.runJob(job);
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    protected void Perform2Means() {
        for (int iteration = 0; iteration < 5; iteration++) {
            
            // Create a JobConf using the conf processed by ToolRunner
            JobConf job = new JobConf(conf, getClass());
            job.setJobName("Gmeans : " + n + " : 2Means : " + iteration);

            String job_input = this.output_path + "it" + n;

            if (n == 0) {
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

            job.setInt("n", n);
            job.setInt("iteration", iteration);

            try {
                JobClient.runJob(job);
            } catch (IOException ex) {
                Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
}
