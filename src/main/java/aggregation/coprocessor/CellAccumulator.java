package aggregation.coprocessor;

import org.apache.hadoop.hbase.Cell;

public interface CellAccumulator {
    boolean hasData();
    void reset();
    CellAccumulatorState canAccumulate(Cell cell);
    void accumulate(Cell cell);
    Cell get();
}
