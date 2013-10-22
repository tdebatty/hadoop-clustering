/* To run :
 * /path/to/bin/hadoop jar 
 *     /path/to/hadoop-clustering.jar
 *     kmeans.Main
 *     -libjars /path/to/spymemcached-2.9.1.jar 
 *     /input_file
 * 
 * */
package kmeans;

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
        Kmeans kmeans = new Kmeans(getConf());
        kmeans.input_path = args[0];
        kmeans.k = 4;
        kmeans.iterations = 5;
        kmeans.run();

        return 0;
    }
}
