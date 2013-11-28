package multikmeans;

import java.io.IOException;
import java.util.Iterator;
import kmeans.Point;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class MultiKmeansReducer
        extends MapReduceBase
        implements Reducer<Text, Point, NullWritable, NullWritable>{

    @Override
    public void reduce(
            Text key,
            Iterator<Point> values,
            OutputCollector<NullWritable, NullWritable> output,
            Reporter reporter) throws IOException {
        
        Point new_center;
        new_center = new Point();
        
        while (values.hasNext()) {
            new_center.addPoint(values.next());
        }
        new_center.reduce();
        
        CacheWrite(key.toString(), new_center.toString());
        
    }
    
}
