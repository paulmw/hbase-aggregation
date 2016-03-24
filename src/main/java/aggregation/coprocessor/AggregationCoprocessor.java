package aggregation.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class AggregationCoprocessor extends BaseRegionObserver {

    private AggregatorHandler buildAggregatorHandler() {
        List<CellAccumulator> accumulators = new ArrayList<>();
        accumulators.add(new DefaultCellAccumulator("stats", "sum", new StringSumAccumulator()));
        accumulators.add(new DefaultCellAccumulator("stats", "max", new StringMaxAccumulator()));
        MultiCellAccumulator acc = new MultiCellAccumulator(accumulators);
        return new AggregatorHandler(acc);
    }

    @Override
    public InternalScanner preFlush(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner) throws IOException {
        return new AggregatingScanner(scanner, buildAggregatorHandler());
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType, CompactionRequest request) throws IOException {
        return new AggregatingScanner(scanner, buildAggregatorHandler());
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner, ScanType scanType) throws IOException {
        return new AggregatingScanner(scanner, buildAggregatorHandler());
    }


    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) throws IOException {
        AggregatorHandler aggregator = buildAggregatorHandler();
        Map<String, String> attributes = new HashMap<>();
        for(Map.Entry<String, byte []> entry : get.getAttributesMap().entrySet()) {
            attributes.put(entry.getKey(), Bytes.toString(entry.getValue()));
        }
        if(attributes.containsKey("aggregate") && attributes.get("aggregate").equals("true")) {
            List<Cell> original = new ArrayList<>(results);
            results.clear();
            aggregator.aggregate(original, results);
        }
    }

    private Map<InternalScanner,Map<String, String>> scannerAttributes = new WeakHashMap<>();

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        Map<String, String> attributes = new HashMap<>();
        for(Map.Entry<String, byte []> entry : scan.getAttributesMap().entrySet()) {
            attributes.put(entry.getKey(), Bytes.toString(entry.getValue()));
        }
        scannerAttributes.put(s, attributes);
        return s;
    }

    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
        scannerAttributes.remove(s);
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        AggregatorHandler aggregator = buildAggregatorHandler();
        Map<String, String> attributes = scannerAttributes.containsKey(s) ? scannerAttributes.get(s) : Collections.<String, String>emptyMap();
        if(attributes.containsKey("aggregate") && attributes.get("aggregate").equals("true")) {
            List<Cell> originalCells = new ArrayList<Cell>();
            Transformer.getCells(results, originalCells);
            results.clear();
            List<Cell> aggregatedCells = new ArrayList<>();
            aggregator.aggregate(originalCells, aggregatedCells);
            Transformer.getResults(aggregatedCells, results);
        }
        return super.postScannerNext(e, s, results, limit, hasMore);
    }
}
