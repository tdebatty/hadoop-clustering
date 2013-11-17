package gmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * @author tibo
 */
public class Point extends kmeans.Point {
    public boolean found = false;

    @Override
    public String toString() {
        return super.toString() + DELIMITER + found;
    }
    
    /* Static String parser */
    public static Point parse(String string) {
        String[] array_string = string.split(DELIMITER);
        double[] array_double = new double[DIM];
        
        for (int i = 0; i < DIM; i++) {
            array_double[i] = Double.valueOf(array_string[i]);
        }

        Point point = new Point();
        point.value = array_double;
        point.count = 1;
        
        if (array_string.length == (DIM + 1)) {
            // Center id is written in the string...
            point.found = Boolean.valueOf(array_string[DIM]);
        }
        
        return point;
    }
}
