package flink.application.wordcount;

import flink.util.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Splitter extends Metrics implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, Integer, String>>  {

    private static final Logger LOG = LoggerFactory.getLogger(Splitter.class);
    private static final String splitregex = "\\W";

    Configuration config;

    public Splitter(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(Tuple2<String, String> value, Collector<Tuple3<String, Integer, String>> out) {
        super.initialize(config);
        // normalize and split the line
        String[] tokens = value.f0.toLowerCase().split(splitregex);

        // emit the pairs
        for (String token : tokens) {
            if (!StringUtils.isBlank(token)) {
                out.collect(new Tuple3<>(token, 1, value.f1));
            }
        }
        super.calculateThroughput();
    }
}
