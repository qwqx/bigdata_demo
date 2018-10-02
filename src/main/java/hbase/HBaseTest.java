package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;

public class HBaseTest {

    @Test
    public void testCreateTable() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.119.111");

        HBaseAdmin client = new HBaseAdmin(conf);

        HTableDescriptor htable = new HTableDescriptor(TableName.valueOf("mytable"));

        htable.addFamily(new HColumnDescriptor("info"));
        htable.addFamily(new HColumnDescriptor("grade"));

        client.createTable(htable);

        client.close();
    }

    @Test
    public void testPutData() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.119.111");

        HTable client = new HTable(conf, "mytable");

        Put put = new Put(Bytes.toBytes("id001"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("tom"));
        client.put(put);
        client.close();
    }

    @Test
    public void testGetData() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.119.111");

        HTable client = new HTable(conf, "mytable");

        Result result = client.get(new Get(Bytes.toBytes("id001")));

        byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));

        System.out.println(Bytes.toString(name));

        client.close();
    }

    @Test
    public void testScanData() throws IOException {

        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.119.111");

        HTable client = new HTable(conf, "mytable");
        ResultScanner scanner = client.getScanner(new Scan());

        for (Result s : scanner) {
            byte[] name = s.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));

            System.out.println(Bytes.toString(name));
        }
        client.close();


    }
}

