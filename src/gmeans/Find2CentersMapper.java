package gmeans;

import java.io.IOException;
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
public class Find2CentersMapper implements Mapper<LongWritable, Text, LongWritable, Point> {

    @Override
    public void map(
            LongWritable key,
            Text value,
            OutputCollector<LongWritable, Point> collector,
            Reporter reporter) throws IOException {
        
        Point point = Point.parse(value.toString());
        collector.collect(new LongWritable(point.center_id), point);
        
    }

    @Override
    public void configure(JobConf jc) {
        
    }

    @Override
    public void close() throws IOException {
        
    }
}