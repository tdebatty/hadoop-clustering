package multikmeans;

import java.io.IOException;
import kmeans.Point;
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
public class MultiKmeansMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, Text, Point> {

    Point[][] centers;
    private int k_min;
    private int k_max;
    private int k_step;
    private int k_count;
    
    private Point point;
    private Text text;
    
    double distance;
    double shortest_distance;
    int shortest;
    
    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<Text, Point> output,
            Reporter reporter) throws IOException {
        
        point.parse(value.toString());
        
        int k = k_min;
        for (int i = 0; i < k_count; i++) {
            distance = 0;
            shortest_distance = Double.POSITIVE_INFINITY;
            shortest = 0;

            for (int j = 0; j < k; j++) {
                if (centers[i][j] == null) {
                    continue;
                }

                distance = point.distance(centers[i][j]);
                if (distance < shortest_distance) {
                    shortest_distance = distance;
                    shortest = j;
                }
            }
            text.set(k + "_" + shortest);
            output.collect(text, point);
            
            
            k += k_step;
        }
        
        
    }
    
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        
        point = new Point();
        text = new Text();
        
        k_min = job.getInt("k_min", 0);
        k_max = job.getInt("k_max", 0);
        k_step = job.getInt("k_step", Integer.MAX_VALUE);
        
        k_count = (k_max - k_min + 1) / k_step;
        centers = new Point[k_count][];
        
        int k = k_min;
        for (int i = 0; i < k_count; i++) {
            centers[i] = ReadCenters(iteration, k);
            k += k_step;
        }
        
    }
    
}
