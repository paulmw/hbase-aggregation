package aggregation.coprocessor;


import org.apache.hadoop.hbase.util.Bytes;

public class LongAccumulator implements ValueAccumulator {

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
        value += Bytes.toLong(bytes);
        hasData = true;
    }

    @Override
    public byte[] get() {
        return Bytes.toBytes(value);
    }

}