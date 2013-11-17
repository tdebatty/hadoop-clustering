package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.hadoop.io.DoubleWritable;
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
public class TestReducer implements Reducer<LongWritable,DoubleWritable,NullWritable,NullWritable>{
    private JobConf job;
    private int gmeans_iteration;
    private Point[] centers;

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<DoubleWritable> list_of_projections,
            OutputCollector<NullWritable, NullWritable> collector, 
            Reporter reporter) throws IOException {
        
        ArrayList arraylist = new ArrayList();
        while (list_of_projections.hasNext()) {
            // vector can become VERY large :-/
            arraylist.add(list_of_projections.next().get());
        }
        
        
        // because arraylist.toArray() returns a Double[] and not a double[] :-(
        double[] values = new double[arraylist.size()];
        String values_string = "";
        for (int i=0; i< arraylist.size(); i++) {
            values[i] = (double) arraylist.get(i);
            values_string += values[i] + ", ";
        }
        Logger.getLogger(TestReducer.class.getName()).log(Level.INFO, "Not-normalized values: " + values_string);
        
        values = StatUtils.normalize(values);
        
        if (adtest(values)) {
            // values seem to follow a normal law
            // remove 2 new centers
            // keep single old center, and mark as found!
            System.out.println("Found a center : " + centers[(int) center_id.get()]);
            Point center = centers[(int) center_id.get()];
            center.found = true;
            
            writeCenterToCache("IT-" + (gmeans_iteration + 1) + "_CENTER-" + center_id.get(), center.toString());           
            writeCenterToCache("IT-" + (gmeans_iteration + 1) + "_CENTER-" + (center_id.get() + (int) Math.pow(2, gmeans_iteration - 1)), "");
            
        } else {
            System.out.println("Gor further with 2 centers...");
        }
        
        
    }
    
    protected boolean adtest(double[] values) {        
        double critical = 1.092; // Corresponds to alpha = 0.01
        NormalDistribution nd = new NormalDistribution();
        Arrays.sort(values);
        int n = values.length;
        double A2 = -n;
        for (int i = 1; i < n; i++) {
            A2 += - (2.0 * i - 1) / n * (Math.log(nd.cumulativeProbability(values[i-1])) + Math.log(1 - nd.cumulativeProbability(values[n - i])) );
        }
        double A2_star = A2 * (1 + 4 / n - 25 / (n * n));
        
        if (A2_star > critical) {
            return false;
            
        } else {
            return true;
        }
    }
    
    private void writeCenterToCache(String key, String value) throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        memcached.set(key, 0, value);
        memcached.shutdown(5, TimeUnit.SECONDS);
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);
        
        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(TestReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
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
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, key + " : " + value);
            if (value != null && value != "") {
                centers[i] = Point.parse((String) value);
            }
        }

        memcached.shutdown();
    }

    @Override
    public void close() throws IOException {
    }
    
}
