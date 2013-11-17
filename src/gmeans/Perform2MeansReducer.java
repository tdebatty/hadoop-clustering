package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class Perform2MeansReducer implements Reducer<LongWritable, Point, NullWritable, NullWritable> {
    private JobConf job;
    private int gmeans_iteration;
    private Point[] centers;

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<NullWritable, NullWritable> collector,
            Reporter reporter) throws IOException {
        
            // Classical K-means reduce : write new center to cache
            Point new_center = new Point();
            while (points.hasNext()) {
                new_center.addPoint(points.next());
                reporter.progress();
            }
            new_center.reduce();
            if (centers[(int) center_id.get()].found){
                new_center.found = true;
            }
            writeCenterToCache("IT-" + gmeans_iteration + "_CENTER-" + center_id, new_center);      
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);

        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(Perform2MeansAndFind2CentersReducer.class.getName()).log(Level.SEVERE, null, ex);
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
            Logger.getLogger(Perform2MeansAndFind2CentersReducer.class.getName()).log(Level.INFO, key + " : " + value);
            if (value != null && value != "") {
                centers[i] = Point.parse((String) value);
            }
        }

        memcached.shutdown();
    }
    
    private void writeCenterToCache(String key, Point new_center) throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        memcached.set(key, 0, new_center.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }
    
}
