/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
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
        Generator mkd = new Generator(getConf());
        mkd.output = args[0];
        mkd.run();

        return 0;
    }
}
