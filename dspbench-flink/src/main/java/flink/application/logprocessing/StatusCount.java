package flink.application.logprocessing;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class StatusCount extends Metrics implements FlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusCount.class);

    private static Map<Integer, Integer> counts;

    Configuration config;

    public StatusCount(Configuration config) {
        super.initialize(config);
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
        super.initialize(config);
        super.incBoth();
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
}
