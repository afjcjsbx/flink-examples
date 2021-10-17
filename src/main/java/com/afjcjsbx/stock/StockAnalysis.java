package com.afjcjsbx.stock;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class StockAnalysis {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        /**
         * We recommend users to NOT set the runtime mode in their program but to instead set it using
         * the command-line when submitting the application. Keeping the application code configuration-free
         * allows for more flexibility as the same application can be executed in any execution mode.
         */
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        class MyTimestampAssigner implements SerializableTimestampAssigner<Tuple5<String, String, String, Double, Integer>> {
            private final SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

            @Override
            public long extractTimestamp(Tuple5<String, String, String, Double, Integer> value, long l) {
                try {
                    Timestamp ts = new Timestamp(sdf.parse(value.f0 + " " + value.f1).getTime());

                    return ts.getTime();
                } catch (Exception e) {
                    throw new java.lang.RuntimeException("Parsing Error");
                }
            }
        }

        DataStream<Tuple5<String, String, String, Double, Integer>> data = env.readTextFile("src/main/resources/FUTURES_TRADES.txt")
                .map(new MapFunction<String, Tuple5<String, String, String, Double, Integer>>() {
                    public Tuple5<String, String, String, Double, Integer> map(String value) {
                        String[] words = value.split(",");
                        // date,    time,     Name,       trade,                      volume
                        return new Tuple5<>(words[0], words[1], "XYZ", Double.parseDouble(words[2]), Integer.parseInt(words[3]));
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple5<String, String, String, Double, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner(new MyTimestampAssigner()));

        // Compute per window statistics
        DataStream<String> change = data.keyBy((KeySelector<Tuple5<String, String, String, Double, Integer>, String>) value -> value.f2)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new TrackChange());

        StreamingFileSink<String> firstReportSink = StreamingFileSink.forRowFormat(new Path("src/main/resources/Report.txt"),
                new SimpleStringEncoder<String>("UTF-8")).build();
        change.addSink(firstReportSink);

        // Alert when price change from one window to another is more than threshold
        DataStream<String> largeDelta = data.keyBy((KeySelector<Tuple5<String, String, String, Double, Integer>, String>) value -> value.f2)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new TrackLargeDelta(5));

        StreamingFileSink<String> alertSink = StreamingFileSink.forRowFormat(new Path("src/main/resources/Alert.txt"),
                new SimpleStringEncoder<String>("UTF-8")).build();
        largeDelta.addSink(alertSink);


        env.execute("Stock Analysis");
    }

    public static class TrackChange extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
        private transient ValueState<Double> prevWindowMaxTrade;
        private transient ValueState<Integer> prevWindowMaxVol;

        public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
            String windowStart = "";
            String windowEnd = "";
            Double windowMaxTrade = 0.0;
            Double windowMinTrade = 0.0;
            Integer windowMaxVol = 0;
            Integer windowMinVol = 0;

            if (prevWindowMaxTrade.value() == null) prevWindowMaxTrade.update(0.0);
            if (prevWindowMaxVol.value() == null) prevWindowMaxVol.update(0);

            for (Tuple5<String, String, String, Double, Integer> element : input) {
                if (windowStart.isEmpty()) {
                    windowStart = element.f0 + ":" + element.f1;
                    windowMinTrade = element.f3;
                    windowMinVol = element.f4;
                }
                if (element.f3 > windowMaxTrade)
                    windowMaxTrade = element.f3;

                if (element.f3 < windowMinTrade)
                    windowMinTrade = element.f3;

                if (element.f4 > windowMaxVol)
                    windowMaxVol = element.f4;
                if (element.f4 < windowMinVol)
                    windowMinVol = element.f4;

                windowEnd = element.f0 + ":" + element.f1;
            }

            double maxTradeChange = 0.0;
            double maxVolChange = 0.0;

            if (prevWindowMaxTrade.value() != 0) {
                maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
            }
            if (prevWindowMaxVol.value() != 0)
                maxVolChange = ((windowMaxVol - prevWindowMaxVol.value()) * 1.0 / prevWindowMaxVol.value()) * 100;

            String output = windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " + String.format("%.2f", maxTradeChange)
                    + ", " + windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange);

            out.collect(output);
            System.out.println(output);

            prevWindowMaxTrade.update(windowMaxTrade);
            prevWindowMaxVol.update(windowMaxVol);
        }

        public void open(Configuration config) {
            prevWindowMaxTrade = getRuntimeContext().getState(new ValueStateDescriptor<>("prev_max_trade", BasicTypeInfo.DOUBLE_TYPE_INFO));
            prevWindowMaxVol = getRuntimeContext().getState(new ValueStateDescriptor<>("prev_max_vol", BasicTypeInfo.INT_TYPE_INFO));
        }
    }

    public static class TrackLargeDelta extends ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow> {
        private final double threshold;
        private transient ValueState<Double> prevWindowMaxTrade;

        public TrackLargeDelta(double threshold) {
            this.threshold = threshold;
        }

        public void process(String key, Context context, Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {
            if (prevWindowMaxTrade.value() == null) prevWindowMaxTrade.update(0.0);

            Double prevMax = prevWindowMaxTrade.value();
            Double currMax = 0.0;
            String currMaxTimeStamp = "";

            for (Tuple5<String, String, String, Double, Integer> element : input) {
                if (element.f3 > currMax) {
                    currMax = element.f3;
                    currMaxTimeStamp = element.f0 + ":" + element.f1;
                }
            }

            // check if change is more than specified threshold
            Double maxTradePriceChange = ((currMax - prevMax) / prevMax) * 100;

            if (prevMax != 0 &&  // don't calculate delta the first time
                    Math.abs((currMax - prevMax) / prevMax) * 100 > threshold) {
                out.collect("Large Change Detected of " + String.format("%.2f", maxTradePriceChange) + "%" + " (" + prevMax + " - " + currMax + ") at  " + currMaxTimeStamp);
            }
            prevWindowMaxTrade.update(currMax);
        }

        public void open(Configuration config) {
            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>("prev_max", BasicTypeInfo.DOUBLE_TYPE_INFO);
            prevWindowMaxTrade = getRuntimeContext().getState(descriptor);
        }

    }
}
