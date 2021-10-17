package com.afjcjsbx.window;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//TumblingEvent

public class WindowType {
    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple2<Long, String>> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
            public Tuple2<Long, String> map(String s) {
                String[] words = s.split(",");
                return new Tuple2<>(Long.parseLong(words[0]), words[1]);
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, String>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<Long, String>>) (value, l) -> value.f0))
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2)))
                .reduce((ReduceFunction<Tuple2<Long, String>>) (t1, t2) -> {
                    int num1 = Integer.parseInt(t1.f1);
                    int num2 = Integer.parseInt(t2.f1);
                    int sum1 = num1 + num2;
                    Timestamp t = new Timestamp(System.currentTimeMillis());
                    return new Tuple2<>(t.getTime(), "" + sum1);
                });

		StreamingFileSink<Tuple2<Long, String>> writeSink = StreamingFileSink.forRowFormat(new Path("src/main/resources/window"),
				new SimpleStringEncoder<Tuple2<Long, String>>("UTF-8")).build();
		sum.addSink(writeSink);

        // execute program
        env.execute("Window");
    }
}

