package gmeans;

import java.io.IOException;
import java.util.Iterator;
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
public class KMeansReducer 
        extends MapReduceBase
        implements Reducer<LongWritable, Point, NullWritable, NullWritable> {
    
    private int gmeans_iteration;
    private Point[] centers;
    Point new_center = new Point();

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<NullWritable, NullWritable> collector,
            Reporter reporter) throws IOException {
        
        // Classical K-means reduce : write new center to cache
        new_center.init();
        while (points.hasNext()) {
            new_center.addPoint(points.next());
            reporter.progress();
        }
        new_center.reduce();
        if (centers[(int) center_id.get()].found){
            new_center.found = true;
        }
        CacheWrite("IT-" + gmeans_iteration + "_CENTER-" + center_id, new_center.toString());
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        
        gmeans_iteration = job.getInt("gmeans_iteration", 0);
        centers = ReadCenters(gmeans_iteration);
    }   
}
