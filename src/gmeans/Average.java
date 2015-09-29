/* To run :
 * /path/to/bin/hadoop jar 
 *     /path/to/hadoop-clustering.jar
 *     gmeans.Average 
 *     -libjars /home/tibo/Java/spymemcached-2.9.1.jar
 *     input
 */

package gmeans;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



/**
 *
 * @author tibo
 */
public class Average extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new Average(), args);
        System.exit(res);
    }
    private String input_path;
    private Configuration conf;
    private String memcached_server = "127.0.0.1";

    @Override
    public int run(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: gmeans.Average <input path> <memcached servers>");
            return 1;
        }
        
        this.conf = getConf();
        this.input_path = args[0];
        this.memcached_server = args[1];
        this.compute();
        
        return 0;
    }    
    
    
    public int compute() {
        try {
            JobConf job = new JobConf(conf, getClass());
            job.setJobName("Gmeans : Average");
            System.out.println("Gmeans : Average");

            FileInputFormat.setInputPaths(job, new Path(this.input_path));
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(AverageMapper.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setReducerClass(AverageReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormat(NullOutputFormat.class);

            job.set("memcached_server", memcached_server);

            JobClient.runJob(job);
            
        } catch (IOException ex) {
            Logger.getLogger(Average.class.getName()).log(Level.SEVERE, null, ex);
            return 1;
        }
        
        
        MemcachedClient memcached;
        
        try {
            memcached = new MemcachedClient(AddrUtil.getAddresses(memcached_server));
        } catch (IOException ex) {
            Logger.getLogger(Gmeans.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not connect to Memcached server!");
            return 1;
        }
        
        int gmeans_last_iteration = (int) memcached.get("gmeans_last_iteration");
        int number_centers = (int) Math.pow(2, gmeans_last_iteration);
        
        double average;
        average = 0;
        int real_number_centers = 0;
        
        for (int i = 0; i < number_centers; i++) {
            Object average_partial_object = memcached.get("gmeans_average_" + i);
            if (average_partial_object == null) {
                continue;
            }
            
            System.out.print((String) average_partial_object + "; ");
            
            average += Double.valueOf((String) average_partial_object);
            real_number_centers++;
        }
        
        System.out.println("");
        System.out.println("Sum of Average distance to center : " + average);
        System.out.println("Number of clusters : " + real_number_centers);
        
        return 0;
    }
}

class AverageMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, DoubleWritable>
{
    private Point[] centers;

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, DoubleWritable> output,
            Reporter reporter) throws IOException {
        
        Point point = new Point();
        point.parse(value.toString());
        
        double distance = 0;
        double shortest_distance = Double.POSITIVE_INFINITY;
        int shortest = 0;
        
        for (int i = 0; i < centers.length; i++) {
            if (centers[i] == null) {
                continue;
            }
            
            distance = point.distance(centers[i]);
            if (distance < shortest_distance) {
                shortest_distance = distance;
                shortest = i;
            }
        }
        
        output.collect(new LongWritable(shortest), new DoubleWritable(shortest_distance));
        
    }
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        int gmeans_last_iteration = (int) CacheRead("gmeans_last_iteration");
        centers = ReadCenters(gmeans_last_iteration);
    }   
}

class AverageReducer
        extends MapReduceBase
        implements Reducer<LongWritable, DoubleWritable, NullWritable, NullWritable> {

    @Override
    public void reduce(
            LongWritable key,
            Iterator<DoubleWritable> values,
            OutputCollector<NullWritable, NullWritable> output,
            Reporter reporter) throws IOException {
        
        double total;
        total = 0;
        long count;
        count = 0;
        
        while (values.hasNext()) {
            total += values.next().get();
            count++;
        }
        
        double average;
        average = total / count;
        
        CacheWrite("gmeans_average_" + key.get(), String.valueOf(average));
        
    }
}