package cn.oyohotels;


import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ExecutorService;

import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import javax.annotation.Nullable;


/**
 * Hello world!
 *
 */
public class Flink2HDFS {
    public static void main(String[] args) throws Exception {

        // final ExecutionEnvironment env =
        // ExecutionEnvironment.getExecutionEnvironment();
       // System.loadLibrary("gplcompression");
        //String topic = "oyo-infra-prod-logs-jar";
        String topic = "oyo-prod-json-jar";
        String group_id = "test";
        String hdfs_path = "";
        String app_name = "test";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
       // CheckpointConfig config = env.getCheckpointConfig();
        //config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",
                "10.200.10.108:9092,10.200.10.109:9092,10.200.10.110:9092");
        prop.setProperty("group.id", group_id);
        prop.setProperty("ack","0");

        FlinkKafkaConsumer010<String> source = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop);
        source.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            long currentTimeStamp = 0l;
            long maxDelayAllowed = 0l;
            long currentWaterMark;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                currentWaterMark = currentTimeStamp - maxDelayAllowed;
                return new Watermark(currentWaterMark);
            }

            @Override
            public long extractTimestamp(String s, long l) {
                String[] arr = s.split(",");
                long timeStamp = Long.parseLong(arr[1]);
                currentTimeStamp = Math.max(timeStamp, currentTimeStamp);
                System.out.println("Key:" + arr[0] + ",EventTime:" + timeStamp + ",水位线:" + currentWaterMark);
                return timeStamp;
            }
        });

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop));


        stream.print();
     //   stream.process(new HbaseSink()).setParallelism(5);
/*        stream.map(new MapFunction<String, String>() {
            public String map(String value)throws IOException {

                return value;
            }

        }).setParallelism(5).process(new HbaseSink()).setParallelism(5);*/
        // DataSet<String> text = env.fromElements("Who's there?", "I think I hear them.
        // Stand, ho! Who's there?");

        /*
         * DataSet<Tuple2<String, Integer>> wordCounts = text .flatMap(new
         * LineSplitter()) .groupBy(0) .sum(1);
         */

      //  DataStream<Tuple2<IntWritable, Text>> line = stream.flatMap(new LineSplitter());

 /*       BucketingSink<Tuple2<IntWritable, Text>> sink = new BucketingSink<Tuple2<IntWritable, Text>>(hdfs_path);
        sink.setBucketer(new DateTimeBucketer<Tuple2<IntWritable, Text>>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")));
        sink.setWriter(new SequenceFileWriter<IntWritable, Text>("com.hadoop.compression.lzo.LzoCodec",
                SequenceFile.CompressionType.BLOCK));
        // sink.setWriter(new StringWriter());
        sink.setBatchSize(1024 * 1024 * 1024L); // this is 400 MB,
        sink.setBatchRolloverInterval(60 * 60 * 1000L); // this is 60 mins
        sink.setPendingPrefix("");
        sink.setPendingSuffix("");
        sink.setInProgressPrefix(".");
        line.addSink(sink);*/

/*        Map<String, String> config1 = new HashMap<>();
        config1.put("cluster.name", "es-for-log");
        config1.put("bulk.flush.max.actions", "1");
        List<InetSocketAddress> transportAddresses = new ArrayList<>();
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.200.20.147"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.200.20.148"), 9300));
        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.200.20.149"), 9300));

        stream.addSink(new ElasticsearchSink<>(config1, transportAddresses, new ElasticsearchSinkFunction<String>() {
            public IndexRequest createIndexRequest(String element) {
                Map<String, String> json = new HashMap<>();
                json.put("data", element);
                System.out.println(json);
                return Requests.indexRequest()
                        .index("test-json-2019.11.27")
                        .type("test-Flink")
                        .source(json);
            }
            @Override
            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        }));*/
        env.execute(app_name);
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<IntWritable, Text>> {
        public void flatMap(String line, Collector<Tuple2<IntWritable, Text>> out) {
            // for (String word : line.split(" ")) {
            //	if (! line.contains("INFO")) {

            System.out.println(line);
            out.collect(new Tuple2<IntWritable, Text>(new IntWritable(1), new Text(line)));
            //	}
            // }
        }
    }

    public static class LineMap implements MapFunction<String, String> {

        public String map(String value) throws Exception {
            // TODO Auto-generated method stub

            return value;

        }
    }

    public static class LineFlatMap implements FlatMapFunction<String, String> {

        public void flatMap(String value, Collector<String> out) throws Exception {
            // TODO Auto-generated method stub

            if (!value.contains("INFO")) {
                out.collect(value);
            }

        }

    }

}