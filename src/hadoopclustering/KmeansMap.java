package hadoopclustering;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.spy.memcached.MemcachedClient;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
 *
 * @author tibo
 */
public class KmeansMap implements Mapper<LongWritable, Text, LongWritable, Point> {

    protected Point[] centers;
    protected JobConf job;
    protected int iteration;
    protected int k;

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, Point> output,
            Reporter reporter) throws IOException {

        Point point = Point.parse(value.toString());

        double distance = 0;
        double shortest_distance = Double.POSITIVE_INFINITY;
        int shortest = 0;

        for (int i = 0; i < centers.length; i++) {

            distance = point.distance(centers[i]);
            if (distance < shortest_distance) {
                shortest_distance = distance;
                shortest = i;
            }
        }

        output.collect(new LongWritable(shortest), point);
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.iteration = job.getInt("iteration", 0);
        this.k = job.getInt("k", 0);

        try {
            readCentersFromCache();
        } catch (IOException ex) {
            Logger.getLogger(KmeansMap.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void close() throws IOException {
    }

    private void readCentersFromCache() throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        String mc_key;
        
        centers = new Point[k];
        for (int i = 0; i < k; i++) {
            mc_key = "center_" + iteration + "_" + i;
            centers[i] = Point.parse((String) memcached.get(mc_key));
        }

        memcached.shutdown();
    }
}
