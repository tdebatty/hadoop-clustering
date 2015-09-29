package dataset;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 * @author tibo
 */
class GeneratorRecordReader implements RecordReader<NullWritable, Text>{
    private final int points_per_task;
    private int current_point = 0;
    private final Center[] centers;
    private float sum_of_weights;

    GeneratorRecordReader(JobConf job) {
        points_per_task = job.getInt(GeneratorInputFormat.POINTS_PER_SPLIT, -1);
        centers = Center.parseArray(job.get(GeneratorInputFormat.CENTERS));
        
        sum_of_weights = 0;
        for (Center center : centers) {
            sum_of_weights += center.weight;
        }        
    }

    @Override
    public boolean next(NullWritable key, Text value) throws IOException {
        if (current_point == points_per_task) {
            return false;
        }
        
        float next_center = current_point % sum_of_weights;
        
        int i = -1;
        // 0.0 <= next_center < sum_of_weights
        while (next_center >= 0) {
            i++;
            if (i >= centers.length) {
                break;
            }
            next_center -= centers[i].weight;
        }
        
        if (i >= centers.length) {
            i = centers.length - 1;
        }
        
        value.set(centers[i].nextPoint());
        
        current_point++;
        return true;
    }

    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    @Override
    public Text createValue() {
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        return current_point;
    }

    @Override
    public void close() throws IOException {
        
    }

    @Override
    public float getProgress() throws IOException {
        return (float) current_point / points_per_task;
    }
    
}
