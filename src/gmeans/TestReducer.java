package gmeans;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
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
public class TestReducer 
        extends MapReduceBase
        implements Reducer<LongWritable,DoubleWritable,NullWritable,NullWritable>{

    private int gmeans_iteration;
    private Point[] centers;

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<DoubleWritable> list_of_projections,
            OutputCollector<NullWritable, NullWritable> collector, 
            Reporter reporter) throws IOException {
        
        ArrayList<Double> arraylist = new ArrayList<>();
        while (list_of_projections.hasNext()) {
            arraylist.add(list_of_projections.next().get());
        }
        
        // because arraylist.toArray() returns a Double[] and not a double[] :-(
        double[] values = new double[arraylist.size()];
        for (int i=0; i< arraylist.size(); i++) {
            values[i] = (double) arraylist.get(i);
        }
        
        values = StatUtils.normalize(values);
        
        if (adtest(values, reporter)) {
            // values seem to follow a normal law
            // remove 2 new centers
            // keep single old center, and mark as found!
            
            Point center = centers[(int) center_id.get()];
            center.found = true;
            
            // Mark center as found at current iteration
            CacheWrite("IT-" + gmeans_iteration + "_CENTER-" + center_id.get(), center.toString());
            CacheWrite("IT-" + gmeans_iteration + "_CENTER-" + (center_id.get() + (int) Math.pow(2, gmeans_iteration - 1)), "");
            
        }
        
        
    }
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        gmeans_iteration = job.getInt("gmeans_iteration", 0);
        centers = ReadCenters(gmeans_iteration - 1);
    }
}
