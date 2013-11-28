package gmeans;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class KMeansAndFindNewCentersCombiner
        extends MapReduceBase
        implements Reducer<LongWritable, Point, LongWritable, Point> {
    
    Point new_center = new Point();
    
    @Override
    public void reduce(
            LongWritable key, Iterator<Point> points,
            OutputCollector<LongWritable, Point> output,
            Reporter reporter) throws IOException {
        
        if (key.get() < KMeansAndFindNewCentersMapper.OFFSET) {
            // Classical K-means combine : reduce and emit point
            new_center.init();
            while (points.hasNext()) {
                new_center.addPoint(points.next());
                reporter.progress();
            }
            new_center.reduce();
            output.collect(key, new_center);
            
        } else {
            // Keep only 2 centers per cluster
            if (points.hasNext()) {
                output.collect(key, points.next());
            }
            
            if (points.hasNext()) {
                output.collect(key, points.next());
            }
        }
    }
}
