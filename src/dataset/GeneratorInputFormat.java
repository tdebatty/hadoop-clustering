/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataset;

import java.io.IOException;
import java.util.Random;
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
    public static final String DIMENSIONALITY = "dataset.generator.dimensionality";
    public static final String NUM_CENTERS = "dataset.generator.centers";
    public static final String NUM_POINTS = "dataset.generator.points";
    
    public static final String POINTS_PER_SPLIT = "dataset.generator.points.per.split";
    public static final String CENTERS = "dataset.generator.centers";

    static void setDimensionality(JobConf job, int dim) {
        job.setInt(DIMENSIONALITY, dim);
    }

    static void setNumCenters(JobConf job, int num_centers) {
        job.setInt(NUM_CENTERS, num_centers);
    }

    static void setNumPoints(JobConf job, int num_points) {
        job.setInt(NUM_POINTS, num_points);
    }
    
    

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        
        numSplits = 4;
        
        int num_centers = job.getInt(NUM_CENTERS, -1);
        int dim = job.getInt(DIMENSIONALITY, -1);
        
        Center[] centers = GenCenters(dim, num_centers);
        
        System.out.println("Generated centers:");
        for (int i = 0; i < num_centers; i++) {
            System.out.println(centers[i]);
        }
        
        String centers_string = Center.serializeArray(centers);
        job.set(CENTERS, centers_string);
        job.setInt(POINTS_PER_SPLIT, job.getInt(NUM_POINTS, -1) / numSplits);
        
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

    private Center[] GenCenters(int dim, int num_centers) {
        Random r = new Random();
        Center[] centers = new Center[num_centers];
        
        for (int i = 0; i < num_centers; i++) {
            Center center = new Center();
            
            // Value
            center.value = new double[dim];
            for (int j = 0; j < dim; j++) {
                center.value[j] = r.nextDouble() * 1000000; // 0 .. 1000.000
            }
            
            // Std Dev
            center.stdDev = new double[dim];
            for (int j = 0; j < dim; j++) {
                center.stdDev[j] = r.nextDouble() * 10; // 0 .. 10
            }
            
            // Weight
            center.weight = r.nextInt(10) + 1; // 1 .. 10
            
            centers[i] = center;
        }
        return centers;
        
    }
    
}

