/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataset;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.mapred.InputSplit;

/**
 *
 * @author tibo
 */
public class FakeInputSplit implements InputSplit {

    @Override
    public long getLength() throws IOException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
    }
    
}
