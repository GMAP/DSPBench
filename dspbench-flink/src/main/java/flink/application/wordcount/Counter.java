package flink.application.wordcount;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Counter extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(Counter.class);
    Configuration config;
    private final Map<String, MutableLong> counts = new HashMap<>();

    Metrics metrics = new Metrics();

    public Counter(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        //super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) {
        //super.initialize(config);
        //super.incBoth();
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }

        String word = value.getField(0);
        MutableLong count = counts.get(word);

        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();

        out.collect(new Tuple2<String, Integer>(word, Math.toIntExact(count.getValue())));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
