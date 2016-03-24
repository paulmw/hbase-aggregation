package aggregation.example;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates events and puts the values into HBase using batched put operations.
 * Aggregated values are computed both at query time and during flush / compaction operations.
 */
public class BatchedAccumulateExample {

    private Table table;
    private Generator generator;

    public void connect() throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        table = connection.getTable(TableName.valueOf("sensors-acc"));
        generator = new Generator(268435456);
    }

    private List<Put> puts = new ArrayList<>();

    private long timestamp = 1;

    private void loadEvent(Event event) throws IOException {
        if(puts.size() == 5000) {
            table.put(puts);
            puts.clear();
        } else {
            Put put = new Put(Bytes.toBytes(Long.toString(event.getSensorID())), timestamp);
            timestamp++;
            put.addColumn("stats".getBytes(), "sum".getBytes(), Bytes.toBytes(event.getValue() + ""));
            put.addColumn("stats".getBytes(), "max".getBytes(), Bytes.toBytes(event.getValue() + ""));
            puts.add(put);
        }
    }

    public void start() throws IOException {
        System.out.println("Loading events...");
        long start = System.currentTimeMillis();
        for(int i = 0; i < 1000000; i++) {
            Event event = generator.getNext();
            loadEvent(event);
            if(i % 10000 == 0 && i > 0) {
                long time = System.currentTimeMillis();
                double delta = (time - start) / 1000.0d;
                double rate = 10000 / delta;
                start = time;
                System.out.println("Inserted 10000 updates in " + delta + " seconds (rate = " + rate + " eps)");
            }
        }
        table.put(puts);
    }

    public static void main(String[] args) throws IOException {
        BatchedAccumulateExample ex1 = new BatchedAccumulateExample();
        ex1.connect();
        ex1.start();
    }
}
