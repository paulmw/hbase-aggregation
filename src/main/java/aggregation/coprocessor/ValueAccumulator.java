package aggregation.coprocessor;


public interface ValueAccumulator {
    boolean hasData();
    void reset();
    void accumulate(byte[] t);
    byte [] get();
}
