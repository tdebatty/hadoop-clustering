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
    private int sum_of_weights;

    GeneratorRecordReader(JobConf job) {
        points_per_task = job.getInt(GeneratorInputFormat.POINTS_PER_SPLIT, -1);
        centers = Center.parseArray(job.get(GeneratorInputFormat.CENTERS));
        sum_of_weights = 0;
        for (int i = 0; i < centers.length; i++) {
            sum_of_weights += centers[i].weight;
        }        
    }

    @Override
    public boolean next(NullWritable key, Text value) throws IOException {
        if (current_point == points_per_task) {
            return false;
        }
        
        int next_center = current_point % sum_of_weights;
        
        int i = 0;
        while ((next_center -= centers[i].weight) > 0) {
            i++;
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
