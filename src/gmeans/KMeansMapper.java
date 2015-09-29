package gmeans;

import java.io.IOException;
import java.util.Random;
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
public class KMeansMapper
        extends MapReduceBase
        implements Mapper<LongWritable, Text, LongWritable, Point> {    

    private int gmeans_iteration;
    private Point[] centers;
    private LongWritable lw = new LongWritable();
    private Point point = new Point();

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, Point> output,
            Reporter reporter) throws IOException {
        
        
        point.parse(value.toString());
        
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
        lw.set(shortest);
        output.collect(lw, point);
        
    }

    @Override
    public void configure(JobConf job) {
        super.configure(job);
        gmeans_iteration = job.getInt("gmeans_iteration", 0);
        centers = ReadCenters(gmeans_iteration);
    }
}
