/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gmeans;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
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
public class Perform2MeansReducer implements Reducer<LongWritable, Point, NullWritable, NullWritable>{
    private JobConf job;
    private int kmeans_iteration;
    private int gmeans_iteration;

    /**
     * Will write new center to 
     * IT-<gmeans_iteration>_2MEANS_<kmeans_iteration>_<center_id>
     * 
     * @param center_id
     * @param points
     * @param collector
     * @param reporter
     * @throws IOException 
     */
    @Override
    public void reduce(
            LongWritable center_id,
            Iterator<Point> points,
            OutputCollector<NullWritable, NullWritable> collector,
            Reporter reporter) throws IOException {
        
        Point new_center = new Point();
        while (points.hasNext()) {
            new_center.addPoint(points.next());
            reporter.progress();
        }
        new_center.reduce();
        writeCenterToCache("IT-" + gmeans_iteration + "_2MEANS_" + kmeans_iteration + "_" + center_id, new_center);
        
    }

    @Override
    public void configure(JobConf job) {
        this.job = job;
        this.kmeans_iteration = job.getInt("kmeans_iteration", 0);
        this.gmeans_iteration = job.getInt("gmeans_iteration", 0);
    }

    @Override
    public void close() throws IOException {
        
    }
    
    private void writeCenterToCache(String key, Point new_center) throws IOException {
        MemcachedClient memcached = new MemcachedClient(
                new InetSocketAddress("127.0.0.1", 11211));

        memcached.set(key, 0, new_center.toString());
        memcached.shutdown(5, TimeUnit.SECONDS);
    }
    
}
