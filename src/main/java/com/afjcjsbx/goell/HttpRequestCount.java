package com.afjcjsbx.goell;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
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

        // print the results with a single thread, rather than in parallel
        text.print().setParallelism(1);

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
        server = ServerBootstrap.bootstrap().setListenerPort(port).registerHandler(path, new HttpRequestHandler() {

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