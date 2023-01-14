package flink.application.clickanalytics;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisitStats extends Metrics implements FlatMapFunction<Tuple3<String, String, String>, Tuple2<Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(VisitStats.class);

    private static int total = 0;
    private static int uniqueCount = 0;

    Configuration config;

    public VisitStats(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(Tuple3<String, String, String> input, Collector<Tuple2<Integer, Integer>> out) {
        super.initialize(config);
        super.incBoth();
        boolean unique = Boolean.parseBoolean(input.getField(2));
        total++;
        if(unique) uniqueCount++;
        out.collect( new Tuple2<Integer, Integer>(total, uniqueCount));
    }
}
