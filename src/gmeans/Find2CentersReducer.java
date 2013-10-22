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
public class Find2CentersReducer implements Reducer<LongWritable, Point, NullWritable, NullWritable> {
    int gmeans_iteration;
    
    /**
     * key: IT-<gmeans_iteration>_FIND_<center_id> 
     * and :IT-<gmeans_iteration>_FIND_<center_id + 2 ^ (gmeans_iteration-1)>
     * @param center_id
     * @param points
     * @param collector
     * @param reporter
     * @throws IOException 
     */
    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<NullWritable, NullWritable> collector,
            Reporter reporter) throws IOException {
        
        // Write first point
        String key = "IT-" + gmeans_iteration + "_FIND_" + center_id;
        WriteToCache(key, points.next());
        
        // Write second point
        key = "IT-" + gmeans_iteration + "_FIND_" + (int) (center_id.get() + Math.pow(2, (gmeans_iteration-1))) ;
        WriteToCache(key, points.next());
        
    }

    @Override
    public void configure(JobConf jc) {
        gmeans_iteration = jc.getInt("gmeans_iteration", 0);
    }

    @Override
    public void close() throws IOException {
        
    }
    
    protected void WriteToCache(String key, Point point) throws IOException {
        Logger.getLogger(Find2CentersMapper.class.getName()).log(Level.INFO, "Write " + key + " : " + point.toString());
        
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        memcached.set(key, 0, point.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }

    
}
