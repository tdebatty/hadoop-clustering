/* To run :
 * /opt/hadoop-1.2.1/bin/hadoop jar 
 *     ./dist/hadoop-clustering.jar
 *     kmeans.Main
 *     -libjars /home/tibo/Java/spymemcached-2.9.1.jar 
 *     /synthetic.data
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
        kmeans.run();

        return 0;
    }
}