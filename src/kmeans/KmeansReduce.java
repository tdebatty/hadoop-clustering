package kmeans;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
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
public class KmeansReduce implements Reducer<LongWritable, Point, NullWritable, NullWritable> {

    protected JobConf job;
    private int iteration;
    private int k;
    protected String memcached_servers = "127.0.0.1";

    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<NullWritable, NullWritable> out,
            Reporter reporter) throws IOException {

        Point new_center = new Point();
        while (points.hasNext()) {
            new_center.addPoint(points.next());
            reporter.progress();
        }
        new_center.reduce();
        writeCenterToCache(center_id.get(), new_center);
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

    private void writeCenterToCache(long center_id, Point new_center) throws IOException {
        MemcachedClient memcached = new MemcachedClient(AddrUtil.getAddresses(memcached_servers));

        String mc_key = "center_" + (iteration + 1) + "_" + center_id;
        memcached.set(mc_key, 0, new_center.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }
}
