package gmeans;

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
    
    @Override
    protected void parse(String[] array_string) {
        super.parse(array_string);
        
        if (array_string.length == (DIM + 1)) {
            found = Boolean.valueOf(array_string[DIM]);
        }
    }
}
