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
    private int kmeans_iterations;
    private Point[] centers;
    private ArrayRealVector[] vectors;

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, DoubleWritable> collector,
            Reporter reporter) throws IOException {
        
        Point point = Point.parse(value.toString());
        ArrayRealVector point_vector = new ArrayRealVector(point.value);
        
        ArrayRealVector vector = vectors[(int) point.center_id];
        
        double projection = vector.dotProduct(point_vector);
        projection = projection / vector.getNorm();
        
        collector.collect(new LongWritable(point.center_id), new DoubleWritable(projection));
        
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
        
        computeVectors();
    }

    @Override
    public void close() throws IOException {
        
    }
    
    private void readCentersFromCache() throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));
        
        int number_centers = (int) Math.pow(2, gmeans_iteration);
        
        String key = "";
        
        // Prefix for last kmeans iteration
        String  prefix = "IT-" + gmeans_iteration + "_2MEANS_" + kmeans_iterations + "_";
        
        Object value;
        centers = new Point[number_centers];
        for (int i = 0; i < number_centers; i++) {
            key = prefix + i;
            value = memcached.get(key);
            if (value != null) {
                centers[i] = Point.parse((String) value);
            }
            Logger.getLogger(TestMapper.class.getName()).log(Level.INFO, "Found " + key + " : " + value);
        }

        memcached.shutdown();
    }

    private void computeVectors() {
        int number_vectors = (int) Math.pow(2, gmeans_iteration - 1);
        vectors = new ArrayRealVector[number_vectors];
        for (int i=0; i<number_vectors; i++) {
            if (centers[i] == null) {
                continue;
            }
            
            ArrayRealVector v1 = new ArrayRealVector(centers[i].value);
            ArrayRealVector v2 = new ArrayRealVector(centers[i + (int) Math.pow(2, gmeans_iteration - 1)].value);
            vectors[i] = v1.subtract(v2);
            
            Logger.getLogger(TestMapper.class.getName()).log(Level.INFO, "Vector computed : " + vectors[i].toString());
        }
                
    }
    
}
