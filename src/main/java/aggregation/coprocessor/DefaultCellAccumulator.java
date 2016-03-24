package aggregation.coprocessor;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

public class DefaultCellAccumulator implements CellAccumulator {

    private ValueAccumulator valueAccumulator;
    private boolean hasData;

    private byte [] row;
    private byte [] family;
    private byte [] qualifier;
    private byte [] tags;
    private byte type;
    private long timestamp;

    private byte [] targetFamily;
    private byte [] targetQualifier;

    public DefaultCellAccumulator(Map<String, String> parameters, ValueAccumulator valueAccumulator) {
        if(parameters.containsKey("family")) {
            this.targetFamily = Bytes.toBytes(parameters.get("family"));
        }
        if(parameters.containsKey("qualifier")) {
            this.targetQualifier = Bytes.toBytes(parameters.get("qualifier"));
        }
        this.valueAccumulator = valueAccumulator;
    }

    public DefaultCellAccumulator(String family, String qualifier, ValueAccumulator valueAccumulator) {
        this.targetFamily = Bytes.toBytes(family);
        this.targetQualifier = Bytes.toBytes(qualifier);
        this.valueAccumulator = valueAccumulator;
    }

    private boolean isTargetCell(Cell cell) {
        return GroupComparatorUtils.compareFamily(targetFamily, cell) == 0 &&
                GroupComparatorUtils.compareQualifier(targetQualifier, cell) == 0;
    }

    private boolean isAccumulatableNow(Cell cell) {
        return GroupComparatorUtils.compareRow(row, cell) == 0 &&
                GroupComparatorUtils.compareTags(tags, cell) == 0 &&
                GroupComparatorUtils.compareTypeByte(type, cell) == 0;
    }

    @Override
    public CellAccumulatorState canAccumulate(Cell cell) {
        if(!hasData) {
            if(isTargetCell(cell)) {
                return CellAccumulatorState.YES;
            } else {
                return CellAccumulatorState.NO;
            }
        } else {
            if(isTargetCell(cell)) {
                if(isAccumulatableNow(cell)) {
                    return CellAccumulatorState.YES;
                } else {
                    return CellAccumulatorState.FUTURE;
                }
            } else {
                return CellAccumulatorState.NO;
            }
        }
    }

    @Override
    public boolean hasData() {
        return hasData;
    }

    @Override
    public void reset() {
        row = null;
        family = null;
        qualifier = null;
        timestamp = -1;
        tags = null;
        valueAccumulator.reset();
        hasData = false;
    }

    @Override
    public void accumulate(Cell cell) {
        if(!hasData) {
            row = CellUtil.cloneRow(cell);
            family = CellUtil.cloneFamily(cell);
            qualifier = CellUtil.cloneQualifier(cell);
            tags = CellUtil.getTagArray(cell);
            type = cell.getTypeByte();
            timestamp = cell.getTimestamp();
            valueAccumulator.accumulate(CellUtil.cloneValue(cell));
            hasData = true;
        } else {
            valueAccumulator.accumulate(CellUtil.cloneValue(cell));
        }
    }

    @Override
    public Cell get() {
        return CellUtil.createCell(row, family, qualifier, timestamp, type, valueAccumulator.get(), tags, 0);
    }

    public String toString() {
        if(hasData) {
            return get().toString();
        } else {
            return "<empty>";
        }
    }
}
