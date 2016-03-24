package aggregation.coprocessor;


import org.apache.hadoop.hbase.util.Bytes;

public class StringSumAccumulator implements ValueAccumulator {

    private long value;
    private boolean hasData;

    @Override
    public boolean hasData() {
        return hasData;
    }

    @Override
    public void reset() {
        value = 0l;
        hasData = false;
    }

    @Override
    public void accumulate(byte[] bytes) {
        value += Long.parseLong(Bytes.toString(bytes));
        hasData = true;
    }

    @Override
    public byte[] get() {
        return Bytes.toBytes(Long.toString(value));
    }

}