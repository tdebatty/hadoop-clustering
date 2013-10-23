/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataset;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;

/**
 *
 * @author tibo
 */

public class GeneratorInputFormat implements InputFormat<NullWritable, Text> {
    public static final float SEPARATION_MEDIUM = 2.0f;
    
    public static final String DIMENSIONALITY = "dataset.generator.dimensionality";
    public static final String NUM_CENTERS = "dataset.generator.centers";
    public static final String SEPARATION = "dataset.generator.separation";
    public static final String NUM_POINTS = "dataset.generator.points";
    public static final String POINTS_PER_TASK = "dataset.generator.points.per.task";
    
    
    public static final String CENTERS = "dataset.generator.centers.value";

    static void setDimensionality(JobConf job, int dim) {
        job.setInt(DIMENSIONALITY, dim);
    }

    static void setNumCenters(JobConf job, int num_centers) {
        job.setInt(NUM_CENTERS, num_centers);
    }

    static void setSeparation(JobConf job, float separation) {
        job.setFloat(SEPARATION, separation);
    }

    static void setNumPoints(JobConf job, int num_points) {
        job.setInt(NUM_POINTS, num_points);
    }
    
    
    private JobConf job;
    private Center[] centers;
    private int num_centers;
    private int dim;
    

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        this.job = job;
        this.num_centers = job.getInt(NUM_CENTERS, -1);
        this.dim = job.getInt(DIMENSIONALITY, -1);
        job.setInt(POINTS_PER_TASK, job.getInt(NUM_POINTS, -1) / numSplits);
        
        ComputeCenters();
        //ComputeDistances();
        //ComputeStdDevs();
        ComputeWeights();
        SerializeCenters();
        
        InputSplit[] splits = new InputSplit[numSplits];
        for (int i = 0; i < numSplits; i++) {
            splits[i] = new FakeInputSplit();
        }
        return splits;
    }

    @Override
    public RecordReader<NullWritable, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new GeneratorRecordReader(job);
    }

    private void ComputeCenters() {
        
        centers = new Center[num_centers];
        
        for (int i = 0; i < num_centers; i++) {
            Center center = new Center();
            center.value = new double[dim];
            for (int j = 0; j < dim; j++) {
                center.value[j] = i;
            }
            centers[i] = center;
        }
        
    }

    private void ComputeDistances() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void ComputeStdDevs() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private void ComputeWeights() {
        for (int i = 0; i < num_centers; i++) {
            centers[i].weight = i;
        }
    }

    private void SerializeCenters() {
        String r = centers[0].toString();
        for (int i = 1; i < centers.length; i++) {
            r += ":" + centers[i].toString();
        }
        job.set(CENTERS, r);
    }
    
}

