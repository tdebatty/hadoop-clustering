/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package multikmeans;

import java.io.IOException;
import java.util.Iterator;
import kmeans.Point;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
public class MultiKmeansCombiner
        extends MapReduceBase implements Reducer<Text, Point, Text, Point> {

    @Override
    public void reduce(
            Text key,
            Iterator<Point> values,
            OutputCollector<Text, Point> output,
            Reporter reporter) throws IOException {
        
        
        Point new_center;
        new_center = new Point();
        
        while (values.hasNext()) {
            new_center.addPoint(values.next());
        }
        new_center.reduce();
        output.collect(key, new_center);
    }
    
}
