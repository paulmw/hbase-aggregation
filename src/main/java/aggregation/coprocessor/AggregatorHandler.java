package aggregation.coprocessor;

import org.apache.hadoop.hbase.Cell;

import java.io.IOException;
import java.util.List;


/**
 * Offline aggregator designed to process a list of Cells.
 */
public class AggregatorHandler {

    private CellAccumulator cellAccumulator;

    public AggregatorHandler(CellAccumulator cellAccumulator) {
        this.cellAccumulator = cellAccumulator;
    }

    public void aggregate(List<Cell> pre, List<Cell> post) throws IOException {
        cellAccumulator.reset();
        for(Cell cell : pre) {
            CellAccumulatorState state = cellAccumulator.canAccumulate(cell);
            if (state.equals(CellAccumulatorState.YES)) {
                cellAccumulator.accumulate(cell);
            } else { // NO or FUTURE
                // If we have accumulated data, deal with that first
                if(cellAccumulator.hasData()) {
                    Cell aggregated = cellAccumulator.get();
                    post.add(aggregated);
                    cellAccumulator.reset();
                }
                // Now deal with the incoming cell
                if(state.equals(CellAccumulatorState.FUTURE)) {
                    // We previously got a FUTURE response, so this cell can be accumulated.
                    // Now that we've cleared the accumulator, lets just double check that
                    // it's now a YES...
                    state = cellAccumulator.canAccumulate(cell);
                    if(!state.equals(CellAccumulatorState.YES)) {
                        throw new IllegalStateException();
                    }
                    cellAccumulator.accumulate(cell);
                } else {
                    // We got a NO, so this cell is just copied out
                    post.add(cell);
                }
            }
        }

        if(cellAccumulator.hasData()) {
            Cell aggregated = cellAccumulator.get();
            post.add(aggregated);
            cellAccumulator.reset();
        }
    }

}
