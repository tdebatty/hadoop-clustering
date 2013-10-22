/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package dataset;

import org.apache.commons.math3.distribution.NormalDistribution;

/**
 *
 * @author tibo
 */
public class Center {

    public static Center parse(String string) {
        System.out.println("Parsing single center : " + string);
        Center center = new Center();
        
        String[] array_string = string.split(",");
        double[] value = new double[array_string.length - 1];
        NormalDistribution[] nds = new NormalDistribution[array_string.length - 1];
        
        
        for (int i = 0; i < (array_string.length - 1); i++) {
            value[i] = Double.valueOf(array_string[i]);
            nds[i] = new NormalDistribution(value[i], 1);
        }
        center.value = value;
        center.distributions = nds;
        center.weight = Integer.valueOf(array_string[array_string.length - 1]);
        return center;
        
    }
    
    public double[] value;
    public int weight = 1;
    private NormalDistribution[] distributions;

    @Override
    public String toString() {
        String r = String.valueOf(value[0]);
        for (int i = 1; i < value.length; i++) {
            r += "," + String.valueOf(value[i]);
        }
        r += "," + String.valueOf(weight);
        return r;
    }
    
    public static Center[] parseAll(String s) {
        System.out.println("Parse centers from " + s);
        String[] array_string = s.split(":");
        System.out.println("Found " + array_string.length + " centers");
        
        Center[] centers = new Center[array_string.length];
        
        for (int i = 0; i < array_string.length; i++) {
            centers[i] = Center.parse(array_string[i]);
        }
        
        return centers;
    }

    String nextPoint() {
        String r = "" + distributions[0].sample();
        for (int i = 1; i < distributions.length; i++) {
            r += "," + distributions[i].sample();
        }
        return r;
    }
    
}
