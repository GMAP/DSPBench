package flink.application.wordcount;

import flink.constants.BaseConstants;
import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class WCInfSource extends RichParallelSourceFunction<Tuple1<String>> {
    private volatile boolean isRunning = true;
    private String sourcePath;
    private long runTimeSec;
    Configuration config;

    private static final Logger LOG = LoggerFactory.getLogger(WCInfSource.class);

    Metrics metrics = new Metrics();

    public WCInfSource(Configuration config, String prefix) {
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
    public void run(SourceContext<Tuple1<String>> ctx) throws Exception {
        try {

            long epoch = System.nanoTime();
            Scanner scanner = new Scanner(new File(sourcePath));

            while (isRunning && (System.nanoTime() - epoch < runTimeSec * 1e9)) {
                String line = scanner.nextLine();
                if(!scanner.hasNextLine()){
                    scanner = new Scanner(new File(sourcePath));
                }
                if (!StringUtils.isBlank(line)){
                    if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                        metrics.emittedThroughput();
                    }
                    ctx.collect(new Tuple1<>(line));
                }
            }

            scanner.close();

            isRunning = false;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
