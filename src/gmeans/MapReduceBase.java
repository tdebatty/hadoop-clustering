package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.hadoop.mapred.JobConf;

/**
 *
 * @author tibo
 */
public class MapReduceBase {
    public static final double CRITICAL = 1.092; // Corresponds to alpha = 0.01
    
    private MemcachedClient memcached;
    protected JobConf job;
    
    public void configure(JobConf job) {
        this.job = job;
                
        try {
            memcached = new MemcachedClient(new InetSocketAddress("127.0.0.1", 11211));
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
    
    protected Point[] ReadCenters(int iteration) {
        int number_centers = (int) Math.pow(2, iteration);
        String prefix = "IT-" + iteration + "_CENTER-";
        String key;
        Object value;
        String value_s;
        
        Point[] centers = new Point[number_centers];
        for (int i = 0; i < number_centers; i++) {
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
    
    protected boolean adtest(double[] values) {        
        
        double A2_star = a2star(values);
        
        if (A2_star > CRITICAL) {
            return false;
            
        } else {
            return true;
        }
    }
    
    protected double a2star(double[] values) {
        NormalDistribution nd = new NormalDistribution();
        Arrays.sort(values);
        int n = values.length;
        double A2 = -n;
        for (int i = 1; i < n; i++) {
            A2 += - (2.0 * i - 1) / n * (Math.log(nd.cumulativeProbability(values[i-1])) + Math.log(1 - nd.cumulativeProbability(values[n - i])) );
        }
        return A2 * (1 + 4 / n - 25 / (n * n));
    }
    
    protected ArrayRealVector[] computeVectors(Point[] centers) {
        int num_vectors = centers.length / 2;
        ArrayRealVector[] vectors = new ArrayRealVector[num_vectors];
        for (int i=0; i<num_vectors; i++) {
            if (centers[i] == null || centers[i].found) {
                continue;
            }
            
            ArrayRealVector v1 = new ArrayRealVector(centers[i].value);
            ArrayRealVector v2 = new ArrayRealVector(centers[i + num_vectors].value);
            vectors[i] = v1.subtract(v2);
            
        }
        return vectors;        
    }
}
