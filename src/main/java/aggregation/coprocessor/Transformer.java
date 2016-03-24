package aggregation.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

public class Transformer {

    public static List<Cell> getCells(List<Result> results, List<Cell> cells) {
        for(Result result : results) {
            for(Cell cell : result.rawCells()) {
                cells.add(cell);
            }
        }
        return cells;
    }

    public static void getResults(List<Cell> cells, List<Result> results) {
        List<Cell> currentCells = null;
        for(int i = 0; i < cells.size(); i++) {
            Cell cell = cells.get(i);
            if (i == 0) {
                currentCells = new ArrayList<>();
                currentCells.add(cell);
            } else {
                Cell prior = currentCells.get(currentCells.size() - 1);
                if (GroupComparatorUtils.compareRow(cell, prior) == 0) {
                    currentCells.add(cell);
                } else {
                    Result result = Result.create(currentCells);
                    results.add(result);
                    currentCells = new ArrayList<>();
                    currentCells.add(cell);
                }
            }
        }

        if (currentCells != null && !currentCells.isEmpty()) {
            Result result = Result.create(currentCells);
            results.add(result);
        }

    }

}
