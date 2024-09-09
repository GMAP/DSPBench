package flink.application.clickanalytics;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RepeatVisit extends RichFlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RepeatVisit.class);

    private static Map<String, Void> map;

    Configuration config;

    Metrics metrics = new Metrics();

    public RepeatVisit(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    private Map<String, Void>  getVisits() {
        if (map == null) {
            map = new HashMap<>();
        }

        return map;
    }


    @Override
    public void flatMap(Tuple3<String, String, String> input, Collector<Tuple3<String, String, String>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        getVisits();

        String clientKey = input.getField(2);
        String url = input.getField(1);
        String key = url + ":" + clientKey;

        if (map.containsKey(key)) {
            out.collect(new Tuple3<String, String, String>(clientKey, url, Boolean.FALSE.toString()));
        } else {
            map.put(key, null);
            out.collect(new Tuple3<String, String, String>(clientKey, url, Boolean.TRUE.toString()));
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
