package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
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
    int n;
    
    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<NullWritable, NullWritable> collector,
            Reporter reporter) throws IOException {
        
        // Write first point
        String key = "IT" + n + "_CENTER" + center_id + "_0";
        WriteToCache(key, points.next());
        
        // Write second point
        key = "IT" + n + "_CENTER" + (center_id.get() + Math.pow(2, (n-1))) + "_0";
        WriteToCache(key, points.next());
        
    }

    @Override
    public void configure(JobConf jc) {
        n = jc.getInt("n", 0);
    }

    @Override
    public void close() throws IOException {
        
    }
    
    protected void WriteToCache(String key, Point point) throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        memcached.set(key, 0, point.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }

    
}
