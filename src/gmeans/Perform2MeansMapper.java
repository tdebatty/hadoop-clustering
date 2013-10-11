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
    private int iteration;
    private int n;
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
        
        for (int i = 0; i<2; i++) {
            int center_id = original_center_id + i * (int) Math.pow(2, (n-1));
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
        this.iteration = job.getInt("iteration", 0);
        this.n = job.getInt("n", 0);

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

        String key = "";
        int number_centers = (int) Math.pow(2, n);
        
        centers = new Point[number_centers];
        for (int i = 0; i < number_centers; i++) {
            key = "IT" + n + "_CENTER" + i + "_INITIAL_" + iteration;
            centers[i] = Point.parse((String) memcached.get(key));
        }

        memcached.shutdown();
    }
    
}
