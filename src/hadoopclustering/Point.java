package hadoopclustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.io.*;

/**
 *
 * @author tibo
 */
public class Point implements Writable, Serializable {
    public static int DIM = 3;
    public static String DELIMITER = ";";
    
    public long count = 0L;
    public double[] value = new double[DIM];

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public double[] getValue() {
        return value;
    }

    public void setValue(double[] value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < DIM; i++) {
            out.writeDouble(value[i]);
        }

        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < DIM; i++) {
            value[i] = in.readDouble();
        }
        count = in.readLong();
    }

    // Mandatory for TextOutputFormat
    @Override
    public String toString() {
        String r = "" + value[0];
        for (int i = 1; i < DIM; i++) {
            r += DELIMITER + value[i];
        }

        return r;
    }

    public static Point parse(String string) {
        String[] array_string = string.split(DELIMITER);
        double[] array_double = new double[array_string.length];
        
        for (int i = 0; i < DIM; i++) {
            array_double[i] = Double.valueOf(array_string[i]);
        }

        Point point = new Point();
        point.value = array_double;
        point.count = 1;
        return point;
    }

    double distance(Point other) {
        double distance = 0;
        for (int i = 0; i < DIM; i++) {
            distance += Math.pow(this.value[i] - other.value[i], 2);
        }
        return Math.sqrt(distance);
    }

    void addPoint(Point other) {
        for (int i = 0; i < DIM; i++) {
            value[i] += other.value[i];
        }

        count+= other.count; // if a combiner is used...
    }

    void reduce() {
        for (int i = 0; i < value.length; i++) {
            value[i] = value[i] / count;

        }

        count = 1;
    }
}