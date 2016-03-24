package aggregation.old;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class Test {

    private Table table;

    public void connect() throws IOException {
        Connection connection = ConnectionFactory.createConnection();
        table = connection.getTable(TableName.valueOf("sensors-acc"));;
    }

    private void put() throws IOException {
        Put put = new Put(Bytes.toBytes("r-x"));
        long l = 1;
        put.addColumn("stats".getBytes(), "count".getBytes(), Bytes.toBytes(l));
        table.put(put);
    }

    public void load() throws IOException {
        System.out.println("Loading events...");
        put();
    }

    public static void main(String[] args) throws IOException {
        Test ex1 = new Test();
        ex1.connect();
        ex1.load();
    }
}
