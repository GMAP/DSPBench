package flink.application.smartgrid;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PlugMedianCalc extends Metrics implements FlatMapFunction<Tuple6<Long, String, String, String, Double, Integer>, Tuple4<String, String, Long, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(PlugMedianCalc.class);

    private static Map<String, RunningMedianCalculator> runningMedians;
    private static Map<String, Long> lastUpdatedTsMap;

    Configuration config;

    public PlugMedianCalc(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    private Map<String, RunningMedianCalculator> runMed(){
        if (runningMedians == null) {
            runningMedians = new HashMap<>();
        }

        return runningMedians;
    }

    private Map<String, Long> tsMap(){
        if (lastUpdatedTsMap == null) {
            lastUpdatedTsMap = new HashMap<>();
        }

        return lastUpdatedTsMap;
    }

    @Override
    public void flatMap(Tuple6<Long, String, String, String, Double, Integer> input, Collector<Tuple4<String, String, Long, Double>> out) {
        super.initialize(config);
        runMed();
        tsMap();

        int operation = input.getField(5);
        double value = input.getField(4);
        long timestamp = input.getField(0);
        String key = getKey(input);

        super.incReceived();

        RunningMedianCalculator medianCalc = runningMedians.get(key);
        if (medianCalc == null) {
            medianCalc = new RunningMedianCalculator();
            runningMedians.put(key, medianCalc);
        }

        Long lastUpdatedTs = lastUpdatedTsMap.get(key);
        if (lastUpdatedTs == null) {
            lastUpdatedTs = 0L;
        }

        if (operation == 1) {
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTsMap.put(key, timestamp);
                super.incEmitted();
                out.collect(new Tuple4<>("plugMedianCalculator" ,key, timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }

    private String getKey(Tuple tuple) {
        return tuple.getField(1).toString() + ':' +
                tuple.getField(2).toString() + ':' +
                tuple.getField(3).toString();
    }
}
