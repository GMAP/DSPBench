package flink.application.logprocessing;

import flink.util.Metrics;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class StatusCount extends Metrics implements FlatMapFunction<Tuple7<Object, Object, Long, Object, Object, Object, String>, Tuple3<Integer, Integer, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(StatusCount.class);

    private static Map<Integer, Integer> counts;

    Configuration config;

    public StatusCount(Configuration config) {
        super.initialize(config);
        this.config = config;
        getCount();
    }

    private Map<Integer, Integer>   getCount() {
        if (counts == null) {
            counts = new HashMap<>();
        }
        return counts;
    }

    @Override
    public void flatMap(Tuple7<Object, Object, Long, Object, Object, Object, String> input, Collector<Tuple3<Integer, Integer, String>> out) {
        super.initialize(config);
        getCount();
        int statusCode = input.getField(4);
        int count = 0;

        if (counts.containsKey(statusCode)) {
            count = counts.get(statusCode);
        }

        count++;
        counts.put(statusCode, count);

        out.collect(new Tuple3<Integer, Integer, String>(statusCode, count, input.f6));
        super.calculateThroughput();
    }
}
