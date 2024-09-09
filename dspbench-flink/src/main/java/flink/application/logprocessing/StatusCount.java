package flink.application.logprocessing;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class StatusCount extends RichFlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusCount.class);

    private static Map<Integer, Integer> counts;

    Configuration config;

    Metrics metrics = new Metrics();

    public StatusCount(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    private Map<Integer, Integer>  getCount() {
        if (counts == null) {
            counts = new HashMap<>();
        }
        return counts;
    }

    @Override
    public void flatMap(Tuple6<Object, Object, Long, Object, Object, Object> input, Collector<Tuple2<Integer, Integer>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        getCount();
        int statusCode = input.getField(4);
        int count = 0;

        if (counts.containsKey(statusCode)) {
            count = counts.get(statusCode);
        }

        count++;
        counts.put(statusCode, count);

        out.collect(new Tuple2<Integer, Integer>(statusCode, count));

    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
