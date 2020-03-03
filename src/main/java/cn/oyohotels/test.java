package cn.oyohotels;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import javax.annotation.Nullable;
import java.util.Properties;

public class test {

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",
                "192.168.71.238:9092,192.168.22.147:9092,192.168.23.213:9092");
        prop.setProperty("group.id", "test");
        prop.setProperty("ack","0");
        prop.setProperty("auto.offset.reset","latest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(20000);
       // DataStream<String> stream = env.addSource(new FlinkKafkaConsumer010<String>("test1", new SimpleStringSchema(),prop));
        DataStream<String> stream = env.socketTextStream("localhost", 9900);

        DataStream res = stream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] data = value.split(",");
                return new Tuple2<String,Long>(data[0],Long.parseLong(data[1]));
            }
        });


        res.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String,Long>>() {

            Long currentWaterMark;
            Long currentMaxTimestamp =0L;
            Long maxOutOfOrderness=0L;

            @Override
            public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {

                long timestamp = element.f1;
                System.out.println("Math max: " + Math.max(timestamp, currentMaxTimestamp));
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("Event: " + element.f0 + "Event time: " + element.f1 + " " + "currentWaterMark:" + currentWaterMark);
                return timestamp;
            }

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {

               // System.out.println("currentMaxTimestamp:" + currentMaxTimestamp );
                currentWaterMark = currentMaxTimestamp - maxOutOfOrderness;
                return new Watermark(currentWaterMark);

            }

        }).keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(60))).reduce(new ReduceFunction<Tuple2<String,Long>>() {


            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
            }
        }).print();

        res.keyBy(0).timeWindow(Time.seconds(20)).sum(1).print();


        env.execute();
    }
}
