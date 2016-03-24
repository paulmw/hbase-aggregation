package aggregation.example;


import java.io.*;
import java.util.Arrays;
import java.util.Random;


/**
 * Generates and uses a cumulative distribution function (CDF) to make events in a power-law distribution.
 *
 * See also https://oeis.org/A000120.
 */
public class Generator {

    private Random random = new Random(0);
    private double [] probabilities;

    public Generator(int n) throws IOException {
        String filename = getFilename(n);
        if ((new File(filename).exists())) {
            probabilities = loadProbabilities(filename);
        } else {
            probabilities = buildProbabilities(n, 0.75);
            storeProbabilities(probabilities, filename);
        }
    }

    private String getFilename(int n) {
        return "/tmp/probabilities-" + n + ".dat";
    }

    public double [] buildProbabilities(int n, double p) {

        probabilities = new double[n];

        System.out.println("Generating probabilities...");

        for(int i = 0; i < n; i++) {
            int a = Integer.bitCount(i);
            int b = Integer.bitCount(n - 1 - i);
            probabilities[i] = Math.pow(p, a) * Math.pow(1 - p, b);
        }

        System.out.println("Sorting probabilities...");
        Arrays.sort(probabilities);

        System.out.println("Reversing probabilities...");
        for(int i = 0; i < n / 2; i++) {
            double temp = probabilities[i];
            probabilities[i] = probabilities[probabilities.length - 1 - i];
            probabilities[probabilities.length - 1 - i] = temp;
        }

        System.out.println("Making cumulative probabilities...");
        for(int i = 1; i < n; i++) {
            probabilities[i] = probabilities[i] + probabilities[i - 1];
        }

        return probabilities;
    }

    private void storeProbabilities(double [] probabilities, String filename) throws IOException {
        System.out.println("Saving probabilities to " + filename);
        DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(filename)));
        out.writeInt(probabilities.length);
        for(double p : probabilities) {
            out.writeDouble(p);
        }
        out.close();
    }

    private double [] loadProbabilities(String filename) throws IOException {
        DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
        System.out.println("Loading probabilities from " + filename);
        int size = in.readInt();
        double [] probabilities = new double[size];
        for(int i = 0; i < probabilities.length; i++) {
            probabilities[i] = in.readDouble();
        }
        in.close();
        return probabilities;
    }

    public Event getNext() {
        Event event = new Event();
        double d = random.nextDouble();
        int position = Arrays.binarySearch(probabilities, d);
        if(position < 0) {
            position = (position + 1) * -1;
        }
        event.setSensorID(position);
        event.setValue(random.nextInt(10) + 1);

        return event;
    }

    public static void main(String[] args) throws IOException {
        Generator direct = new Generator(268435456);
        Random random = new Random();
        for(int i = 0; i < 100000000; i++) {
            Event event = direct.getNext();
            if(i % 100000 == 0) {
                System.out.println(i);
            }
        }
    }
}
