package aggregation.old;


import java.util.Arrays;
import java.util.Random;

public class Generator {

    private Random random = new Random();
    private long elements;
    private double alpha = 0.75;

    public Generator(long elements) {
        this.elements = elements;
    }

    public long next() {

        double lower = 0;
        double upper = elements - 1;

        while (Math.round(lower) != Math.round(upper)) {
            double midpoint = lower + ((upper - lower) / 2);
            if(random.nextDouble() < alpha) {
                upper = midpoint;
            } else {
                lower = midpoint;
            }
        }

        return Math.round(lower);
    }

    public static void main(String[] args) {
        int count = 0;
        int [] frequencies = new int [16];
        Generator g = new Generator(frequencies.length);
        for(int i = 0; i < 100000000; i++) {
            count++;
            frequencies[(int) g.next()]++;
        }
        System.out.println(Arrays.toString(frequencies));
    }
}
