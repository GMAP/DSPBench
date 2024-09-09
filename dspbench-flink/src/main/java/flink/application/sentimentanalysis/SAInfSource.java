package flink.application.sentimentanalysis;

import flink.constants.BaseConstants;
import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.Scanner;

public class SAInfSource extends RichParallelSourceFunction<Tuple3<String, String, Date>> {
    private volatile boolean isRunning = true;
    private String sourcePath;
    private long runTimeSec;
    Configuration config;

    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    Metrics metrics = new Metrics();

    public SAInfSource(Configuration config, String prefix) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
        this.sourcePath = config.getString(String.format(BaseConstants.BaseConf.SOURCE_PATH, prefix),"");
        this.runTimeSec = config.getInteger(String.format(BaseConstants.BaseConf.RUNTIME, prefix), 60);
    }

    @Override
    public void cancel() {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
        isRunning = false;
    }

    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String, String, Date>> ctx) throws Exception {
        try {

            long epoch = System.nanoTime();
            Scanner scanner = new Scanner(new File(sourcePath));

            while (isRunning && (System.nanoTime() - epoch < runTimeSec * 1e9)) {
                String line = scanner.nextLine();
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(sourcePath));
                }

                JSONObject tweet = new JSONObject(line);

                if (tweet.has("data")) {
                    tweet = (JSONObject) tweet.get("data");
                }

                if (!tweet.has("id") || !tweet.has("text") || !tweet.has("created_at"))
                    continue;

                String id = (String) tweet.get("id");
                String text = (String) tweet.get("text");
                DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                ctx.collect(new Tuple3<>(id, text, timestamp.toDate()));
            }

            scanner.close();

            isRunning = false;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
