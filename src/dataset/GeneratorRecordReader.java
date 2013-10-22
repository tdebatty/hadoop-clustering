/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataset;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author tibo
 */
class GeneratorRecordReader implements RecordReader<NullWritable, Text>{
    private final JobConf job;
    private final int points_per_task;
    private int current_point = 0;
    private final int dim;
    private final Center[] centers;

    GeneratorRecordReader(JobConf job) {
        this.job = job;
        this.points_per_task = job.getInt(GeneratorInputFormat.POINTS_PER_TASK, -1);
        this.dim = job.getInt(GeneratorInputFormat.DIMENSIONALITY, -1);
        this.centers = Center.parseAll(job.get(GeneratorInputFormat.CENTERS));
        
    }

    @Override
    public boolean next(NullWritable key, Text value) throws IOException {
        if (current_point == points_per_task) {
            return false;
        }
        value.set(centers[current_point % centers.length].nextPoint());
        
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
