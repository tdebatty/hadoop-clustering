package gmeans;

import java.io.IOException;
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
public class TestMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, DoubleWritable>{
    
    private int gmeans_iteration;
    private Point[] centers;
    private ArrayRealVector[] vectors;
    private LongWritable lw = new LongWritable();
    private DoubleWritable dw = new DoubleWritable();
    Point point = new Point();

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, DoubleWritable> collector,
            Reporter reporter) throws IOException {
        
        
        point.parse(value.toString());
        
        // Find cluster (nearest center)
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
        
        // At first iteration:
        // shortest = 0
        // and centers.length == 1
        // and centers[0] == null
        if (centers.length != 1 && centers[shortest].found) {
            // Already found => no need to test...
            return;
        }
        
        // Fetch the corresponding "2-centers" vector
        ArrayRealVector vector = vectors[shortest];
        
        // In some cases, it was not possible to build the vector
        // because one of the 2 centers was missing for example
        if (vector == null) {
            return;
        }
        
        // Make a Math.vector from the point
        ArrayRealVector point_vector = new ArrayRealVector(point.value);
        
        double projection = vector.dotProduct(point_vector);
        dw.set(projection / vector.getNorm());
        lw.set(shortest);
        collector.collect(lw, dw);
        
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        gmeans_iteration = job.getInt("gmeans_iteration", 0);
        
        
        vectors = computeVectors(ReadCenters(gmeans_iteration));
        centers = ReadCenters(gmeans_iteration - 1);
    }   
}
