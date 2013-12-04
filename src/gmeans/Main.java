/* To run :
 * /path/to/bin/hadoop jar 
 *     /path/to/hadoop-clustering.jar
 *     gmeans.Main 
 *     -libjars /home/tibo/Java/spymemcached-2.9.1.jar,/home/tibo/Java/commons-math3-3.2/commons-math3-3.2.jar
 *     input
 *     output
 *     max iterations
 */

package gmeans;

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
        if (args.length != 3) {
            System.out.println("Usage: gmeans.Main <input path> <max gmeans iterations> <memcached server>");
            return 1;
        }
        
        Gmeans gmeans = new Gmeans(getConf());
        gmeans.input_path = args[0];
        gmeans.max_iterations = Integer.valueOf(args[1]);
        gmeans.memcached_server = args[2];
        gmeans.run();
        
        return 0;
    }
}
