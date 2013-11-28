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
public class KMeansCombiner 
        extends MapReduceBase
        implements Reducer<LongWritable, Point, LongWritable, Point> {
    
    Point new_center = new Point();

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<LongWritable, Point> collector,
            Reporter reporter) throws IOException {
        
        // Classical K-means combine
        new_center.init();
        while (points.hasNext()) {
            new_center.addPoint(points.next());
            reporter.progress();
        }
        new_center.reduce();
        collector.collect(center_id, new_center);
        
    }  
}
