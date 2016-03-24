package aggregation.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Online aggregator designed to be put in-line in the
 * flush and compaction processes.OnlineAggregator
 */
public class AggregatingScanner implements InternalScanner {

    private InternalScanner scanner;
    private AggregatorHandler aggregator;

    public AggregatingScanner(InternalScanner scanner, AggregatorHandler aggregator) {
        setScanner(scanner);
        this.aggregator = aggregator;
    }

    public void setScanner(InternalScanner scanner) {
        this.scanner = scanner;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        List<Cell> original = new ArrayList<>();
        boolean shouldContinue = scanner.next(original);
        aggregator.aggregate(original, results);
        return shouldContinue;
    }

    @Override
    public boolean next(List<Cell> results, int limit) throws IOException {
        List<Cell> original = new ArrayList<>();
        boolean shouldContinue = scanner.next(original, limit);
        aggregator.aggregate(original, results);
        return shouldContinue;
    }

    @Override
    public void close() throws IOException {
        scanner.close();
    }
}
