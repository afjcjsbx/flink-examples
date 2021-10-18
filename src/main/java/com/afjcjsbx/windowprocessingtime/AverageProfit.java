package com.afjcjsbx.windowprocessingtime;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageProfit {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        // month, product, category, profit, count
        DataStream<Tuple5<String, String, String, Integer, Integer>> mapped = data.map(new Splitter());      // tuple  [June,Category5,Bat,12,1]
        // groupBy 'month'
        DataStream<Tuple5<String, String, String, Integer, Integer>> reduced = mapped
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .reduce(new Reduce1());

        StreamingFileSink<Tuple5<String, String, String, Integer, Integer>> report = StreamingFileSink.forRowFormat(new Path("src/main/resources/www.txt"),
                new SimpleStringEncoder<Tuple5<String, String, String, Integer, Integer>>("UTF-8")).build();
        reduced.addSink(report).setParallelism(1);

        // execute program
        env.execute("Avg Profit Per Month");
    }

    public static class Reduce1 implements ReduceFunction<Tuple5<String, String, String, Integer, Integer>> {
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> current,
                                                                       Tuple5<String, String, String, Integer, Integer> pre_result) {
            return new Tuple5<>(current.f0,
                    current.f1, current.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
        }
    }

    public static class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        public Tuple5<String, String, String, Integer, Integer> map(String value) {
            String[] words = value.split(",");
            // ignore timestamp, we don't need it for any calculations
            //Long timestamp = Long.parseLong(words[5]);
            return new Tuple5<>(words[1], words[2], words[3], Integer.parseInt(words[4]), 1);
        }
    }

}
