package dataset;

import java.util.Random;

/**
 *
 * @author tibo
 */
public class Center {
    
    public static final String SEPARATOR = ";";
    public static final String ARRAY_SEPARATOR = ":"; // DON't USE "|" !!!

    public static String serializeArray(Center[] centers) {
        String r = centers[0].toString();
        for (int i = 1; i < centers.length; i++) {
            r += ARRAY_SEPARATOR + centers[i].toString();
        }
        return r;
    }
    
    public static Center[] parseArray(String s) {
        String[] array_string = s.split(ARRAY_SEPARATOR);
        
        Center[] centers = new Center[array_string.length];
        
        for (int i = 0; i < array_string.length; i++) {
            centers[i] = new Center();
            centers[i].parse(array_string[i]);
        }
        
        return centers;
    }
    
    
    public double[] value;
    public double[] stdDev;
    public float weight = 1;
    
    private int dim;
    private Random rng;

    @Override
    public String toString() {
        
        // Values
        String r = String.valueOf(value[0]);
        
        for (int i = 1; i < value.length; i++) {
            r += SEPARATOR + String.valueOf(value[i]);
        }
        
        // StdDevs
        for (int i = 0; i < stdDev.length; i++) {
            r += SEPARATOR + String.valueOf(stdDev[i]);
        }
        
        // Weight
        r += SEPARATOR + String.valueOf(weight);
        return r;
    }
    
    public void parse(String string) {
        
        String[] array_string = string.split(SEPARATOR);
        dim = (array_string.length - 1) / 2;
        
        // values
        value = new double[dim];
        for (int i = 0; i < dim; i++) {
            value[i] = Double.valueOf(array_string[i]);
            
        }
        
        // std devs
        stdDev = new double[dim];
        for (int i = 0; i < dim; i++) {
            stdDev[i] = Double.valueOf(array_string[dim + i]);
        }
        
        // weight
        weight = Float.valueOf(array_string[array_string.length - 1]);
        
        // Create rng
        // Java.util.random uses system.nanotime as seed
        // might not be enough => use securerandom to seed
        
        //SecureRandom srng = new SecureRandom();
        //byte[] seed = srng.generateSeed(8);
        
        //ByteBuffer bb = ByteBuffer.wrap(seed);
        //rng = new Random((bb.getLong() + System.nanoTime() + System.currentTimeMillis()) % Long.MAX_VALUE);
        
        rng = new Random();
        
    }

    public String nextPoint() {
        String r = "";
        r += (rng.nextGaussian() * stdDev[0] + value[0]);
        
        for (int i = 1; i < dim; i++) {
            r += ";" + (rng.nextGaussian() * stdDev[i] + value[i]);
        }
        return r;
        
        /*
        String r = "" + distributions[0].sample();
        for (int i = 1; i < distributions.length; i++) {
            r += ";" + distributions[i].sample();
        }
        return r;*/
    }
    
}
