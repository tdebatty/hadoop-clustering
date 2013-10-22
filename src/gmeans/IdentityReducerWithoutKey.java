/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package gmeans;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 *
 * @author tibo
 */
public class IdentityReducerWithoutKey<K,V>
         implements Reducer<K, V, NullWritable, V> {
    
      /** Writes all keys and values directly to output. */

      @Override
      public void reduce(K key, Iterator<V> values,
                         OutputCollector<NullWritable, V> output, Reporter reporter)
        throws IOException {
        while (values.hasNext()) {
          output.collect(NullWritable.get(), values.next());
        }
      }

   

    @Override
    public void configure(JobConf jc) {
        
    }

    @Override
    public void close() throws IOException {
        
    }
}
