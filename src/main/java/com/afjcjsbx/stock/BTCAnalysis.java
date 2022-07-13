package com.afjcjsbx.stock;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class BTCAnalysis {

    public BTCAnalysis() {}

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        BTCAnalysis job = new BTCAnalysis();
        job.execute();
    }

    public JobExecutionResult execute() throws Exception {

        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        /**
         * We recommend users to NOT set the runtime mode in their program but to instead set it using
         * the command-line when submitting the application. Keeping the application code configuration-free
         * allows for more flexibility as the same application can be executed in any execution mode.
         */
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        DataStream<String> data = env.socketTextStream("localhost", 9090);

        DataStream<Tuple3<Long, String, Double>> sum = data
                .map(new MapFunction<String, Tuple3<Long, String, Double>>() {
                    public Tuple3<Long, String, Double> map(String value) {
                        String[] elems = value.split(",");
                        // time,     price
                        System.out.println(value);
                        return new Tuple3<>(Long.parseLong(elems[0]), elems[1], Double.parseDouble(elems[1]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Long, String, Double>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<Long, String, Double>>) (value, l) -> value.f0));

        // Alert when price change from one window to another is more than threshold
        DataStream<String> largeDelta = sum.keyBy((KeySelector<Tuple3<Long, String, Double>, String>) value -> value.f1)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.seconds(15)))
                .process(new TrackLargeDelta(0));

        StreamingFileSink<String> alertSink = StreamingFileSink.forRowFormat(new Path("src/main/resources/Alert.txt"),
                new SimpleStringEncoder<String>("UTF-8")).build();
        largeDelta.addSink(alertSink);

        return env.execute("Stock Analysis");
    }


    public static class TrackLargeDelta extends ProcessWindowFunction<Tuple3<Long, String, Double>, String, String, TimeWindow> {
        private final double threshold;
        private transient ValueState<Double> prevWindowMaxTrade;
        private transient ValueState<Double> prevWindowMinTrade;

        public TrackLargeDelta(double threshold) {
            this.threshold = threshold;
        }

        public void process(String key, Context context, Iterable<Tuple3<Long, String, Double>> input, Collector<String> out) throws Exception {
            if (prevWindowMaxTrade.value() == null) prevWindowMaxTrade.update(0.0);
            if (prevWindowMinTrade.value() == null) prevWindowMinTrade.update(0.0);

            Double prevMax = prevWindowMaxTrade.value();
            Double prevMin = prevWindowMinTrade.value();
            Double currMax = 0.0;
            String currMaxTimeStamp = "";

            for (Tuple3<Long, String, Double> element : input) {
                if (element.f2 > currMax) {
                    System.out.println(element.f2);
                    currMax = element.f2;
                    currMaxTimeStamp = String.valueOf(element.f0);
                }
            }

            // check if change is more than specified threshold
            Double maxTradePriceChange = ((currMax - prevMax) / prevMax) * 100;

            System.out.println(Math.abs((currMax - prevMax) / prevMax) * 100);
            if (prevMax != 0 &&  // don't calculate delta the first time
                    Math.abs((currMax - prevMax) / prevMax) * 100 > threshold) {
                System.out.println("Scrivo");
                out.collect("Large Change Detected of " + String.format("%.2f", maxTradePriceChange) + "%" + " (" + prevMax + " - " + currMax + ") at  " + currMaxTimeStamp);
            }
            prevWindowMaxTrade.update(currMax);
        }

        public void open(Configuration config) {
            ValueStateDescriptor<Double> max = new ValueStateDescriptor<>("prev_max", BasicTypeInfo.DOUBLE_TYPE_INFO);
            ValueStateDescriptor<Double> min = new ValueStateDescriptor<>("prev_min", BasicTypeInfo.DOUBLE_TYPE_INFO);
            prevWindowMaxTrade = getRuntimeContext().getState(max);
            prevWindowMinTrade = getRuntimeContext().getState(min);
        }

    }
}
