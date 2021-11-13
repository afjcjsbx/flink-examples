package com.afjcjsbx.goell;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class HttpRequestCount {

    public static void main(String[] args) throws Exception {

        // the host and the port to connect to
        final String path;
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            path = params.has("path") ? params.get("path") : "*";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount "
                    + "--path <hostname> --port <port>', where path (* by default) "
                    + "and port is the address of the text server");
            System.err.println("To start a simple text server, run 'netcat -l <port>' and "
                    + "type the input text into the command line");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.addSource(new OneHourHttpTextStreamFunction(path, port));

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text

                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        for (String word : value.split("\\s")) {
                            out.collect(new WordWithCount(word, 1L));
                        }
                    }
                })

                .keyBy("word").timeWindow(Time.seconds(5))

                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Http Request Count");
    }

}

class OneHourHttpTextStreamFunction implements SourceFunction<String> {

    private static final long serialVersionUID = 1L;

    private final String path;

    private final int port;

    private transient HttpServer server;

    public OneHourHttpTextStreamFunction(String path, int port) {
        checkArgument(port > 0 && port < 65536, "port is out of range");

        this.path = checkNotNull(path, "path must not be null");
        this.port = port;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        server = ServerBootstrap.bootstrap().setListenerPort(port).registerHandler(path, new HttpRequestHandler(){
.
            @Override
            public void handle(HttpRequest req, HttpResponse rep, HttpContext context) throws HttpException, IOException {
                ctx.collect(req.getRequestLine().getUri());
                rep.setStatusCode(200);
                rep.setEntity(new StringEntity("OK"));
            }
        }).create();
        server.start();
        server.awaitTermination(1, TimeUnit.HOURS);
    }

    @Override
    public void cancel() {
        server.stop();
    }
}