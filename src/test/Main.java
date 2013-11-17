/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.util.Arrays;
import org.apache.commons.math3.distribution.NormalDistribution;

/**
 *
 * @author tibo
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        double[] values = {0.1917,    1.2441,   -1.1642,    1.6353,    0.6863,   -1.1495,    1.1102,    1.1826,    1.7073,   -0.4203,    1.6394,    0.2635,   -1.4635,   -0.7762,    0.9295,    1.7930,   -1.1832,   -0.1112,   -0.6568,    1.8797,   -0.8961,    1.3315,   -0.6246,    0.7767,   -1.3243,    1.5003,   -2.2029,    0.3225,    0.4210,   -0.8207,   -0.9665,    0.6093,   -1.0362,   -0.0282,   -2.8198,   -2.0810,   -0.0448,    1.1180,   -1.6495,    0.6787,    0.4943,   -0.5885,   -0.0239,    2.1913,   -1.4006,    0.4801,    0.2076,    0.3221,   -0.0059,    0.2814,   -0.2513,   -1.6935,   -0.5840,    0.2344,    0.2588,    0.6037,    2.2210,   -1.6541,    0.6804,    0.1358,   -0.0380,   -0.6931,   -0.1275,    0.6804,    0.4262,   -1.6057,    0.9062,    0.2495,    1.8942,   -1.1124,   -0.7220,   -1.7522,    0.0036,    0.9951,   -0.4790,   -0.5191,   -0.2947,   -0.1605,    1.0235,    0.0159,   -0.4827,    0.6347,   -1.3644,    0.5986};
        System.out.println(adtest(values));
        
        double[] values_exp = {0.7918,    4.5890,    0.5124,    0.5082,    0.4317,    1.0708,    0.7066,    0.3541,    0.1190,    2.8994,    2.3191,    0.4311,    0.2691,    0.0121,    2.0768,    1.0093,    0.3912,    0.9788,    0.1468,    1.2311};
        System.out.println(adtest(values_exp));
    }
    
    protected static boolean adtest(double[] values) {
        /*$critical = 1.092; // Corresponds to alpha = 0.01
        $nd = new \webd\stats\NormalDistribution();
        $sorted = $this->sort();
        $n = $this->length();
        $A2 = -$n;
        for ($i = 1; $i <= $n; $i++) {
            $A2 += -(2 * $i - 1) / $n * ( log($nd->cumulativeProbability($sorted->value[$i - 1])) + log(1 - $nd->cumulativeProbability($sorted->value[$n - $i])) );
        }
        $A2_star = $A2 * (1 + 4 / $n - 25 / ($n * $n));
        if ($A2_star > $critical) {
            return FALSE;
        } else {
            // Data seems to follow a normal law
            return TRUE;
        }*/
        
        double critical = 1.092;
        NormalDistribution nd = new NormalDistribution();
        Arrays.sort(values);
        int n = values.length;
        double A2 = -n;
        for (int i = 1; i < n; i++) {
            A2 += - (2.0 * i - 1) / n * (Math.log(nd.cumulativeProbability(values[i-1])) + Math.log(1 - nd.cumulativeProbability(values[n - i])) );
        }
        double A2_star = A2 * (1 + 4 / n - 25 / (n * n));
        
        if (A2_star > critical) {
            return false;
            
        } else {
            return true;
        }
    }
}
