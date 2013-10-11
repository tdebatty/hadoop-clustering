package gmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author tibo
 */
public class Point extends kmeans.Point {
    public long center_id = 0;

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(center_id);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        center_id = in.readLong();
    }

    @Override
    public String toString() {
        return super.toString() + DELIMITER + center_id;
    }
    
    /* Static String parser */
    public static Point parse(String string) {
        String[] array_string = string.split(DELIMITER);
        double[] array_double = new double[array_string.length];
        
        for (int i = 0; i < DIM; i++) {
            array_double[i] = Double.valueOf(array_string[i]);
        }

        Point point = new Point();
        point.value = array_double;
        point.count = 1;
        
        if (array_string.length == (DIM + 1)) {
            // Center id is written in the string...
            point.center_id = Long.valueOf(array_string[DIM]);
        }
        
        return point;
    }
    
    
    
    
    
}
