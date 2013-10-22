package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class Perform2MeansMapper implements Mapper<LongWritable, Text, LongWritable, Point> {
    private JobConf job;
    private int kmeans_iteration;
    private int gmeans_iteration;
    private Point[] centers;

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, Point> output,
            Reporter reporter) throws IOException {
        
        Point point = Point.parse(value.toString());
        
        double distance = 0;
        double shortest_distance = Double.POSITIVE_INFINITY;
        int shortest = 0;
        int original_center_id = (int) point.center_id;
        
        for (int i = 0; i < 2; i++) {
            int center_id = original_center_id + i * (int) Math.pow(2, (gmeans_iteration-1));
            distance = point.distance(centers[center_id]);
            if (distance < shortest_distance) {
                shortest_distance = distance;
                shortest = center_id;
            }
        }
        
        point.center_id = shortest;
        output.collect(new LongWritable(shortest), point);
        
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.kmeans_iteration = job.getInt("kmeans_iteration", 0);
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);

        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(Perform2MeansMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void close() throws IOException {
        
        
    }

    private void readCentersFromCache() throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));
        
        int number_centers = (int) Math.pow(2, gmeans_iteration);
        
        String key = "";
        
        // Prefix for kmeans iteration 1
        String prefix = "IT-" + gmeans_iteration + "_FIND_";
        
        if (kmeans_iteration > 1) {
            // For other kmeans iterations
            prefix = "IT-" + gmeans_iteration + "_2MEANS_" + (kmeans_iteration - 1) + "_";
        }
         
        
        Object value;
        centers = new Point[number_centers];
        for (int i = 0; i < number_centers; i++) {
            key = prefix + i;
            value = memcached.get(key);
            Logger.getLogger(Perform2MeansMapper.class.getName()).log(Level.INFO, key + " : " + value);
            if (value != null) {
                centers[i] = Point.parse((String) value);
            }
        }

        memcached.shutdown();
    }
    
}
