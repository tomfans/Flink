package cn.oyohotels;

/**
 * Hello world!
 *
 */


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.logging.Logger;

/**
 * Hello world!
 *
 */
public class App
{
    // private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main( String[] args ) throws Exception {


        //  System.loadLibrary("gplcompression");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",
                "10.200.10.108:9092,10.200.10.109:9092,10.200.10.110:9092");
        prop.setProperty("batch.size","16384");
        prop.setProperty("buffer.memory","320000");
        prop.setProperty("ack","0");
        prop.setProperty("auto.offset.reset","earliest");
        prop.setProperty("linger.ms","10000");
        prop.setProperty("group.id", "test");

        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<>("oyo-prod-json-jar", new SimpleStringSchema(), prop)).setParallelism(30);
        Map<String, String> config1 = new HashMap<>();
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
        }));

        env.execute("This is a test Flink task");
        //  DataStream<Tuple2<IntWritable, Text>> line = stream.flatMap(new LineSplitter());

        //  line.addSink(sink);



    }
}
