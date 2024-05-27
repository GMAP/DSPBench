package flink.application.clickanalytics;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RepeatVisit extends Metrics implements FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RepeatVisit.class);

    private static Map<String, Void> map;

    Configuration config;

    public RepeatVisit(Configuration config) {
        super.initialize(config);
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
        super.initialize(config);
        super.incBoth();
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
}
