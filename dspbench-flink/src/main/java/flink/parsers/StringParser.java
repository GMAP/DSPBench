package flink.parsers;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Configurations;
import flink.util.Metrics;

public class StringParser extends RichFlatMapFunction<String, Tuple1<String>> {

    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    Configuration config;

    Metrics metrics = new Metrics();

    public StringParser(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple1<String>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        
        if (StringUtils.isBlank(input))
            out.collect(new Tuple1<String>(null));

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }

        out.collect(new Tuple1<String>(input));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

}
