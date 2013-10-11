package kmeans;

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
public class KmeansCombine implements Reducer<LongWritable, Point, LongWritable, Point> {

    protected JobConf job;
    private int iteration;
    private int k;

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<LongWritable, Point> out,
            Reporter reporter) throws IOException {

        Point new_center = new Point();
        while (points.hasNext()) {
            new_center.addPoint(points.next());
            reporter.progress();
        }
        out.collect(center_id, new_center);
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.iteration = job.getInt("iteration", 0);
        this.k = job.getInt("k", 0);
    }

    @Override
    public void close() throws IOException {
        // Nothing to do...
    }
}
