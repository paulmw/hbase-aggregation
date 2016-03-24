package aggregation.coprocessor;


import org.apache.hadoop.hbase.Cell;

import java.util.List;

public class MultiCellAccumulator implements CellAccumulator {

    private List<CellAccumulator> cellAccumulators;
    private CellAccumulator activeCellAccumulator;

    public MultiCellAccumulator(List<CellAccumulator> cellAccumulators) {
        this.cellAccumulators = cellAccumulators;
    }

    @Override
    public boolean hasData() {
        return activeCellAccumulator != null && activeCellAccumulator.hasData();
    }

    @Override
    public void reset() {
        for (CellAccumulator cellAccumulator : cellAccumulators) {
            cellAccumulator.reset();
        }
        activeCellAccumulator = null;
    }

    @Override
    public CellAccumulatorState canAccumulate(Cell cell) {
        if (activeCellAccumulator == null) {
            // There's no active accumulator, so we need to search for one that could accept this cell.
            for (CellAccumulator accumulator : cellAccumulators) {
                CellAccumulatorState state = accumulator.canAccumulate(cell);
                if (state.equals(CellAccumulatorState.YES)) {
                    // We found an accumulator so say yes
                    return state;
                } else {
                    if (state.equals(CellAccumulatorState.FUTURE)) {
                        // We found an accumulator that said future, so it has data, but it's not active. That's an error.
                        throw new IllegalStateException();
                    }
                }
            }
            return CellAccumulatorState.NO;
        } else {
            // There's an active accumulator. If that says yes or future to this cell, we're good.
            CellAccumulatorState state = activeCellAccumulator.canAccumulate(cell);
            if (!state.equals(CellAccumulatorState.NO)) {
                return state;
            } else {
                // The active accumulator said no, so we need to search for any other accumulators that could accept this cell.
                // If any other accumulators could accept this cell, we return future to ensure we will see it again. Otherwise, we just return no.
                for (CellAccumulator accumulator : cellAccumulators) {
                    state = accumulator.canAccumulate(cell);
                    if (state.equals(CellAccumulatorState.YES)) {
                        return CellAccumulatorState.FUTURE;
                    } else if (state.equals(CellAccumulatorState.FUTURE)) {
                        // We found a different accumulator that said future, so it has data, but it's not active. That's an error.
                        throw new IllegalStateException();
                    }
                }
                return CellAccumulatorState.NO;
            }
        }
    }


    @Override
    public void accumulate(Cell cell) {
        if (activeCellAccumulator == null) {
            // There's no active accumulator, so we need to search for one that could accept this cell.
            for (CellAccumulator accumulator : cellAccumulators) {
                CellAccumulatorState state = accumulator.canAccumulate(cell);
                if (state.equals(CellAccumulatorState.YES)) {
                    // We found an accumulator so say yes
                    activeCellAccumulator = accumulator;
                    activeCellAccumulator.accumulate(cell);
                    break;
                } else if (state.equals(CellAccumulatorState.FUTURE)) {
                    // We found an accumulator that said future, so it has data, but it's not active. That's an error.
                    throw new IllegalStateException();
                }
            }
        } else {
            if(activeCellAccumulator.canAccumulate(cell).equals(CellAccumulatorState.YES)) {
                activeCellAccumulator.accumulate(cell);
            } else {
                throw new IllegalStateException("Active accumulator is unable to accumulate cell.");
            }
        }
    }

    @Override
    public Cell get() {
        return activeCellAccumulator.get();
    }
}
