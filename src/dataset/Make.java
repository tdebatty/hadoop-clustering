package dataset;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Make extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new Make(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage : dataset.Make <output dir> <number of points> <number of centers> <dimensionality>");
            return 1;
        }
        
        Generator mkd = new Generator(getConf());
        mkd.output = args[0];
        mkd.num_points = Integer.valueOf(args[1]);
        mkd.num_centers = Integer.valueOf(args[2]);
        mkd.dim = Integer.valueOf(args[3]);
        mkd.run();

        return 0;
    }
}
