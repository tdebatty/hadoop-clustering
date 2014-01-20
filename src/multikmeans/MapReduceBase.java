package multikmeans;

import gmeans.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author tibo
 */
public class MapReduceBase {    
    private MemcachedClient memcached;
    protected JobConf job;
    protected int iteration;
    
    public void configure(JobConf job) {
        this.job = job;
        
        iteration = job.getInt("iteration", 0);
        try {
            memcached = new MemcachedClient(AddrUtil.getAddresses(job.get("memcached_server", "")));
        } catch (IOException ex) {
            Logger.getLogger(MapReduceBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void close() throws IOException {
        memcached.shutdown(5, TimeUnit.SECONDS);
    }
    
    protected void CacheWrite(String key, String value) {
        memcached.set(key, 0, value);
    }
    
    protected Object CacheRead(String key) {
        return memcached.get(key);
    }
    
    protected Point[] ReadCenters(int iteration, int k) {
        String prefix = iteration + "_" + k + "_";
        String key;
        Object value;
        String value_s;
        
        Point[] centers = new Point[k];
        for (int i = 0; i < k; i++) {
            key = prefix + i;
            
            value = CacheRead(key);
            if (value == null) {
                continue;
            }
            
            value_s = (String) value;
            if ("".equals(value_s)) {
                continue;
            }
                   
            centers[i] = new Point();
            centers[i].parse(value_s);
        }
        return centers;
    }
}
