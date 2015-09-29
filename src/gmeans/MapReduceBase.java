package gmeans;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class MapReduceBase {
    //public static final double CRITICAL = 1.092; // 99% certainty
    public static final double CRITICAL = 0.787; // 95% certainty
    
    private MemcachedClient memcached;
    protected JobConf job;
    
    public void configure(JobConf job) {
        this.job = job;
        
        // To avoid hitting the memcached servers too badly
        /*Random rnd = new Random();
        try {
            Thread.sleep(rnd.nextInt(5000));
            
        } catch (InterruptedException ex) {
            //Logger.getLogger(MapReduceBase.class.getName()).log(Level.SEVERE, null, ex);
        }*/
        
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
    
    protected boolean adtest(double[] values, Reporter reporter) {        
        
        double A2_star = a2star(values, reporter);
        return A2_star < CRITICAL;
    }
    
    protected double a2star(double[] values, Reporter reporter) {
        NormalDistribution nd = new NormalDistribution();
        Arrays.sort(values);
        reporter.progress();
        int n = values.length;
        double A2 = -n;
        for (int i = 1; i < n; i++) {
            reporter.progress();
            A2 += - (2.0 * i - 1) / n * (Math.log(nd.cumulativeProbability(values[i-1])) + Math.log(1 - nd.cumulativeProbability(values[n - i])) );
        }
        return A2 * (1 + 4 / n - 25 / (n * n));
    }
    
    protected ArrayRealVector[] computeVectors(Point[] centers) {
        int num_vectors = centers.length / 2;
        ArrayRealVector[] vectors = new ArrayRealVector[num_vectors];
        for (int i=0; i<num_vectors; i++) {
            if (centers[i] == null || centers[i + num_vectors] == null || centers[i].found) {
                continue;
            }
            
            ArrayRealVector v1 = new ArrayRealVector(centers[i].value);
            ArrayRealVector v2 = new ArrayRealVector(centers[i + num_vectors].value);
            vectors[i] = v1.subtract(v2);
            
        }
        return vectors;        
    }
}
