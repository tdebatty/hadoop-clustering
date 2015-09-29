package dataset;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 *
 * @author tibo
 */
class Generator {
    public int dim = 3;
    public int num_centers = 10;
    public int num_points = 10000;

    private final Configuration conf;
    public String output = "";

    Generator(Configuration conf) {
        this.conf = conf;
    }

    public int run() {

        // Create a JobConf using the conf processed by ToolRunner
        JobConf job = new JobConf(conf, getClass());
        job.setJobName("Make Dataset");
        
        job.setInputFormat(GeneratorInputFormat.class);
        GeneratorInputFormat.setDimensionality(job, dim);
        GeneratorInputFormat.setNumCenters(job, num_centers);
        GeneratorInputFormat.setNumPoints(job, num_points);
        
        // No mapper & no reducer
        // => Direct write to disk
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        try {
            JobClient.runJob(job);
        } catch (IOException ex) {
            Logger.getLogger(getClass().getName()).log(Level.SEVERE, null, ex);
            return 1;
        }
        return 0;
    }
}

