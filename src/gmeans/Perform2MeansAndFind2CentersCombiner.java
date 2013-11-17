package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class Perform2MeansAndFind2CentersCombiner implements Reducer<LongWritable, Point, LongWritable, Point> {
    
    private int gmeans_iteration;
    
    private Point[] centers;
    
    @Override
    public void reduce(
            LongWritable key, Iterator<Point> values,
            OutputCollector<LongWritable, Point> output,
            Reporter reporter) throws IOException {
        
        int i = 0;
        Point new_center = new Point();
        while (values.hasNext()) {
            Point point = values.next();
            
            // Collect the 2 first centers in the cluster...
            if (!centers[(int) key.get()].found && i < 2) {
                System.out.println("Collect a new center for cluster " + key.get());
                output.collect(new LongWritable(Perform2MeansAndFind2CentersMapper.OFFSET + key.get()), point);
                i++;
            }
            
            new_center.addPoint(point);
            reporter.progress();
        }
        new_center.reduce();
        output.collect(key, new_center);
    }

    @Override
    public void configure(JobConf job) {
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);

        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(Perform2MeansAndFind2CentersCombiner.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void close() throws IOException {
        
    }
    
    private void readCentersFromCache() throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));
        
        int number_centers = (int) Math.pow(2, gmeans_iteration);
        
        String prefix = "IT-" + gmeans_iteration + "_CENTER-";
        String key;
        Object value;
        
        centers = new Point[number_centers];
        for (int i = 0; i < number_centers; i++) {
            key = prefix + i;
            value = memcached.get(key);
            Logger.getLogger(Perform2MeansAndFind2CentersCombiner.class.getName()).log(Level.INFO, key + " : " + value);
            if (value != null && value != "") {
                centers[i] = Point.parse((String) value);
            }
        }

        memcached.shutdown();
    }
    
}
