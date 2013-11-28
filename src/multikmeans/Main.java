/* To run :
 * /path/to/bin/hadoop jar 
 *     /path/to/hadoop-clustering.jar
 *     multikmeans.Main
 *     -libjars /path/to/spymemcached-2.9.1.jar 
 *     /input
 *     iterations
 * */
package multikmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author tibo
 */
public class Main extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: multikmeans.Main <input path> <iterations>");
            return 1;
        }
        
        MultiKmeans mkmeans = new MultiKmeans(getConf());
        mkmeans.input_path = args[0];
        mkmeans.iterations = Integer.valueOf(args[1]);
        mkmeans.run();

        return 0;
    }
}
