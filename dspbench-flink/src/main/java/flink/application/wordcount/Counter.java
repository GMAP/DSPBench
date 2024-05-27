package flink.application.wordcount;

import flink.util.Metrics;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Counter extends Metrics implements FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(Counter.class);
    Configuration config;
    private final Map<String, MutableLong> counts = new HashMap<>();

    public Counter(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) {
        super.initialize(config);
        super.incBoth();
        String word = value.getField(0);
        MutableLong count = counts.get(word);

        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();

        out.collect(new Tuple2<String, Integer>(word, Math.toIntExact(count.getValue())));
    }
}
