package cn.oyohotels;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

public class test2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

        // env.getConfig().setAutoWatermarkInterval(5000);
        DataStream<String> stream = env.socketTextStream("localhost", 9900).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

            Long currentWaterMark;
            Long currentMaxTimestamp =0L;
            Long maxOutOfOrderness=0L;
            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                currentWaterMark = currentMaxTimestamp - maxOutOfOrderness;
                return new Watermark(currentWaterMark);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {

                String[] data = element.split(",");
                long timestamp = Long.parseLong(data[1]);
                System.out.println("Math max: " + Math.max(timestamp, currentMaxTimestamp));
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                System.out.println("Event: " + data[0] + "Event time: " + data[1] + " " + "currentWaterMark:" + currentWaterMark);
                return timestamp;
            }
        });

        SingleOutputStreamOperator res = stream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] data = value.split(",");

                return new Tuple2<>(data[0], Long.parseLong(data[1]));
            }
        }).keyBy(0).timeWindow(Time.seconds(30)).process(new ProcessWindowFunction<Tuple2<String, Long>, Long, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {

                int key = 0;
                Long total = 0L;
                for(Tuple2<String,Long> in : elements){
                    total += in.f1;
                    key = Integer.parseInt(in.f0);
                }
                if(key != 1111) {
                    out.collect(total);

                }
                context.output(outputTag,"sideout-" + total);
            }
        });

        DataStream<String> sideOutputStream = res.getSideOutput(outputTag);
        res.print();

        sideOutputStream.print();





        try {
            System.out.println(env.getExecutionPlan());

            env.execute("this is a test");
        } catch(Exception ex){
            ex.printStackTrace();
        }

    }
}
