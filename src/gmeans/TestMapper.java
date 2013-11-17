package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.hadoop.io.DoubleWritable;
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
public class TestMapper implements Mapper<LongWritable, Text, LongWritable, DoubleWritable>{
    private JobConf job;
    private int gmeans_iteration;
    private Point[] centers;
    private ArrayRealVector[] vectors;

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, DoubleWritable> collector,
            Reporter reporter) throws IOException {
        
        
        int number_vectors = (int) Math.pow(2, gmeans_iteration - 1);
        
        Point point = Point.parse(value.toString());
        
        // Find shortest center
        double distance = 0;
        double shortest_distance = Double.POSITIVE_INFINITY;
        int shortest = 0;
        
        for (int i = 0; i < centers.length; i++) {
            if (centers[i] == null) {
                continue;
            }
            
            distance = point.distance(centers[i]);
            if (distance < shortest_distance) {
                shortest_distance = distance;
                shortest = i;
            }
        }
        
        if (shortest >= number_vectors) {
            shortest -= number_vectors;
        }
        
        // Make a vector from the point
        ArrayRealVector point_vector = new ArrayRealVector(point.value);
        
        // Fetch the corresponding "2-centers" vector
        ArrayRealVector vector = vectors[shortest];
        
        double projection = vector.dotProduct(point_vector);
        projection = projection / vector.getNorm();
        
        collector.collect(new LongWritable(shortest), new DoubleWritable(projection));
        
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);
        
        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(TestMapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        computeVectors();
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
            Logger.getLogger(this.getClass().getName()).log(Level.INFO, key + " : " + value);
            if (value != null && value != "") {
                centers[i] = Point.parse((String) value);
            }
        }

        memcached.shutdown();
    }

    private void computeVectors() {
        int number_vectors = (int) Math.pow(2, gmeans_iteration - 1);
        vectors = new ArrayRealVector[number_vectors];
        for (int i=0; i<number_vectors; i++) {
            if (centers[i] == null || centers[i].found) {
                continue;
            }
            
            ArrayRealVector v1 = new ArrayRealVector(centers[i].value);
            ArrayRealVector v2 = new ArrayRealVector(centers[i + number_vectors].value);
            vectors[i] = v1.subtract(v2);
            
            Logger.getLogger(TestMapper.class.getName()).log(Level.INFO, "Vector computed : " + vectors[i].toString());
        }
                
    }
    
}
