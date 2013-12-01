package multikmeans;

/**
 *
 * @author tibo
 */
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import kmeans.Point;
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
    
    public static final String AVERAGE_KEY_FORMAT = "kmeans_average_%d_%d_%d";
    
    private String input_path;
    private Configuration conf;
    private int k_min = 1;
    private int k_max = 20;
    private int k_step = 1;
    private int iterations = 5;
    

    @Override
    public int run(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: gmeans.Average <input path>");
            return 1;
        }
        
        this.conf = getConf();
        this.input_path = args[0];
        this.compute();
        
        return 0;
    }    
    
    
    public int compute() {
        try {
            JobConf job = new JobConf(conf, getClass());
            job.setJobName("MultiKMeans : Average");
            System.out.println("MultiKMeans : Average");
            System.out.println("Input pagh: " + input_path);

            FileInputFormat.setInputPaths(job, new Path(this.input_path));
            job.setInputFormat(TextInputFormat.class);

            job.setMapperClass(AverageMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setReducerClass(AverageReducer.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputFormat(NullOutputFormat.class);
            
            job.setInt("k_min", k_min);
            job.setInt("k_max", k_max);
            job.setInt("k_step", k_step);
            job.setInt("iterations", iterations);


            JobClient.runJob(job);
            
        } catch (IOException ex) {
            Logger.getLogger(Average.class.getName()).log(Level.SEVERE, null, ex);
            return 1;
        }
        
        
        MemcachedClient memcached;
        
        try {
            memcached = new MemcachedClient(new InetSocketAddress("127.0.0.1", 11211));
        } catch (IOException ex) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not connect to Memcached server!");
            return 1;
        }
        
        
        double average;
        
        // Loop on iterations
        for (int i=0; i < iterations; i++) {
            System.out.println("Iteration " + i);
            System.out.println("------------------");
            
            // Loop on values of k
            for (int k = k_min; k <= k_max; k+= k_step) {
                average = 0;
                int count = 0;
                // Loop on centers
                for (int c = 0; c < k; c++) {
                    Object mem_object = memcached.get(String.format(AVERAGE_KEY_FORMAT, i, k, c));
                    if (mem_object == null) {
                        continue;
                    }
                    //System.out.println(mem_object);
                    average += Double.valueOf((String) mem_object);
                    count++;
                }
                System.out.println("K=" + k + " | Sum of Avg dist=" + average + " | Real # clusters=" + k);
                
            }
        
        }
        
        
        
        return 0;
    }
}


class AverageMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, DoubleWritable>
{
    private Point[][][] centers;
    private int k_min;
    private int k_max;
    private int k_step;
    private int iterations;
    private int k_count;

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<Text, DoubleWritable> output,
            Reporter reporter) throws IOException {
        
        Point point = new Point();
        point.parse(value.toString());
        
        double distance;
        double shortest_distance;
        int shortest;
        
        // Loop on each iteration
        for (int i = 0; i < iterations; i++) {
            
            // Loop on values of k
            int j = 0;
            for (int k = k_min; k <= k_max; k += k_step) {
                distance = 0;
                shortest_distance = Double.POSITIVE_INFINITY;
                shortest = 0;
                
                // Loop on centers
                for (int c = 0; c < k; c++) {
                    if (centers[i][j][c] == null) {
                        continue;
                    }

                    distance = point.distance(centers[i][j][c]);
                    if (distance < shortest_distance) {
                        shortest_distance = distance;
                        shortest = c;
                    }
                }
                
                output.collect(new Text(String.format("%d_%d_%d", i, k, shortest)), new DoubleWritable(shortest_distance));
                
                j++;
            }
            
        }
        
        
        
    }
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        k_min = job.getInt("k_min", -1);
        k_max = job.getInt("k_max", -1);
        k_step = job.getInt("k_step", 1);
        iterations = job.getInt("iterations", 0);
        
        k_count = (k_max - k_min + 1) / k_step;
        
        centers = new Point[iterations+1][][];
        for (int i = 0; i <= iterations; i++) {
            centers[i] = new Point[k_count][];
            int j = 0;
            for (int k = k_min; k <= k_max; k += k_step) {
                centers[i][j] = ReadCenters(i, k);
                
                j++;
            }
        }
    }
    
}

class AverageReducer
        extends MapReduceBase
        implements Reducer<Text, DoubleWritable, NullWritable, NullWritable> {

    @Override
    public void reduce(
            Text key,
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
        
        CacheWrite("kmeans_average_" + key.toString(), String.valueOf(average));
        
    }
}
