package gmeans;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.stat.StatUtils;
import org.apache.hadoop.io.BooleanWritable;
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
public class TestFewClustersMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, DoubleWritable>{
    
    private int gmeans_iteration;
    private Point[] centers;
    private ArrayRealVector[] vectors;
    private ArrayList[] projections;
    private OutputCollector collector;
    private Point point = new Point();
    
    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, DoubleWritable> collector,
            Reporter reporter) throws IOException {
        
        
        // Keep a reference to the collector, for use in close
        if (this.collector == null) {
            this.collector = collector;
        }
        
        
        point.parse(value.toString());
        
        // Find cluster (nearest center)
        double distance = 0;
        double shortest_distance = Double.POSITIVE_INFINITY;
        int nearest = 0;
        
        for (int i = 0; i < centers.length; i++) {
            if (centers[i] == null) {
                continue;
            }
            
            distance = point.distance(centers[i]);
            if (distance < shortest_distance) {
                shortest_distance = distance;
                nearest = i;
            }
        }
        
        // At first iteration:
        // shortest = 0
        // and centers.length == 1
        // and centers[0] == null
        if (centers.length != 1 && centers[nearest].found) {
            // Already found => no need to test...
            return;
        }
        
        // Make a Math.vector from the point
        ArrayRealVector point_vector = new ArrayRealVector(point.value);
        
        // Fetch the corresponding "2-centers" vector and compute projection
        ArrayRealVector vector = vectors[nearest];
        double projection = vector.dotProduct(point_vector) / vector.getNorm();
        
        if (projections[nearest] == null) {
            projections[nearest] = new ArrayList<>();
        }
        
        projections[nearest].add(projection);
        
        
    }


    @Override
    public void configure(JobConf job) {
        super.configure(job);
        gmeans_iteration = job.getInt("gmeans_iteration", 0);       
        vectors = computeVectors(ReadCenters(gmeans_iteration));
        centers = ReadCenters(gmeans_iteration - 1);
        projections = new ArrayList[centers.length];
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        
        for (int vectorid = 0; vectorid < projections.length; vectorid++) {
            if (projections[vectorid] == null) {
                continue;
            }
            
            // TODO : add a minimal threshold!
            
            double[] values = new double[projections[vectorid].size()];
            for (int i=0; i < projections[vectorid].size(); i++) {
                values[i] = (double) projections[vectorid].get(i);
            }
            values = StatUtils.normalize(values);
            double a2star = a2star(values);
            collector.collect(new LongWritable(vectorid), new DoubleWritable(a2star));
            
        }
    }
}
