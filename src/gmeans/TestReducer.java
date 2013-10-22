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
    private int kmeans_iterations;
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
            // => don't keep new centers
            
        } else {
            Logger.getLogger(TestReducer.class.getName()).log(Level.INFO, "2 new centers!");
            // values don't seem to follow a normal law
            // Write new centers to cache
            // key IT-<gmeans_iteration>_TEST_<center_id>
            String key = "IT-" + gmeans_iteration + "_TEST_" + center_id;
            writeCenterToCache(key, centers[(int) center_id.get()]);
            
            int second_center_id = (int) (center_id.get() + Math.pow(2, gmeans_iteration-1));
            key = "IT-" + gmeans_iteration + "_TEST_" + second_center_id;
            writeCenterToCache(key, centers[second_center_id]);
        }
        
        
    }
    
    protected boolean adtest(double[] values) {
        /*$critical = 1.092; // Corresponds to alpha = 0.01
        $nd = new \webd\stats\NormalDistribution();
        $sorted = $this->sort();
        $n = $this->length();
        $A2 = -$n;
        for ($i = 1; $i <= $n; $i++) {
            $A2 += -(2 * $i - 1) / $n * ( log($nd->cumulativeProbability($sorted->value[$i - 1])) + log(1 - $nd->cumulativeProbability($sorted->value[$n - $i])) );
        }
        $A2_star = $A2 * (1 + 4 / $n - 25 / ($n * $n));
        if ($A2_star > $critical) {
            return FALSE;
        } else {
            // Data seems to follow a normal law
            return TRUE;
        }*/
        
        double critical = 1.092;
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
    
    private void writeCenterToCache(String key, Point new_center) throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        memcached.set(key, 0, new_center.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);
        this.kmeans_iterations = job.getInt("kmeans_iterations", 0);
        
        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(Perform2MeansMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
    }
    
    private void readCentersFromCache() throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));
        
        int number_centers = (int) Math.pow(2, gmeans_iteration);
        
        String key = "";
        
        // Prefix for kmeans iteration 1
        String  prefix = "IT-" + gmeans_iteration + "_2MEANS_" + (kmeans_iterations - 1) + "_";
        
        Object value;
        centers = new Point[number_centers];
        for (int i = 0; i < number_centers; i++) {
            key = prefix + i;
            value = memcached.get(key);
            if (value != null) {
                centers[i] = Point.parse((String) value);
            }
        }

        memcached.shutdown();
    }

    @Override
    public void close() throws IOException {
        
        
        
    }
    
}
