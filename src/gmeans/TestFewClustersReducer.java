package gmeans;

import java.io.IOException;
import java.util.Iterator;
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
public class TestFewClustersReducer 
        extends MapReduceBase
        implements Reducer<LongWritable,DoubleWritable,NullWritable,NullWritable>{

    private int gmeans_iteration;
    private Point[] centers;

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<DoubleWritable> list_of_a2star,
            OutputCollector<NullWritable, NullWritable> collector, 
            Reporter reporter) throws IOException {
        
        double a2star_total = 0;
        int a2star_count = 0;
        while (list_of_a2star.hasNext()) {
            double a2star = list_of_a2star.next().get();
            //System.out.println(a2star);
            a2star_total += a2star;
            a2star_count++;
        }
        
        double a2star_average = a2star_total / a2star_count;
        
        if (a2star_average < CRITICAL) {
            // values seem to follow a normal law
            // remove 2 new centers
            // keep single old center, and mark as found!
            
            Point center = centers[(int) center_id.get()];
            center.found = true;
            
            // Mark this center as found
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
