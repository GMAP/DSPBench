package flink.application.wordcount;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splitter extends RichFlatMapFunction<Tuple1<String>, Tuple2<String, Integer>>  {

    private static final Logger LOG = LoggerFactory.getLogger(Splitter.class);
    private static final String splitregex = "\\W";

    Configuration config;

    Metrics metrics = new Metrics();

    public Splitter(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(Tuple1<String> value, Collector<Tuple2<String, Integer>> out) {
        //super.initialize(config);
        //super.incReceived();
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        // normalize and split the line
        String[] tokens = value.f0.toLowerCase().split(splitregex);

        // emit the pairs
        for (String token : tokens) {
            if (!StringUtils.isBlank(token)) {
                //super.incEmitted();
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple2<>(token, 1));
            }
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
