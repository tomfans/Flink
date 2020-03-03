package cn.oyohotels;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HbaseSink extends ProcessFunction<String,String> {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseSink.class);

    private String _zookeeper;
    private String _port;
    private String _tableName;
    private Table htable;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        try {
            Configuration conf = HBaseConfiguration.create();
            conf.set(HConstants.ZOOKEEPER_QUORUM, "10.201.20.143,10.201.20.144,10.201.20.145");
            conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
            Connection connection = ConnectionFactory.createConnection(conf);

            htable = connection.getTable(TableName.valueOf("test"));

            LOGGER.error("[HbaseSink] : open HbaseSink finished");
        } catch (Exception e) {
            LOGGER.error("[HbaseSink] : open HbaseSink faild {}", e);
        }
    }

    @Override
    public void close() throws Exception {
        htable.close();
    }

    @Override
    public void processElement(String  value, Context ctx, Collector<String > out)
            throws Exception {
        LOGGER.error("process String {}",value);
        String rowId = Long.toString(System.currentTimeMillis());
        Put put = new Put(Bytes.toBytes(rowId));
      //  put.setDurability(Durability.ASYNC_WAL);
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("data"), Bytes.toBytes(value));
        htable.put(put);
        LOGGER.error("[HbaseSink] : put rowKey:{}, value:{} to hbase", rowId, value);
    }



}