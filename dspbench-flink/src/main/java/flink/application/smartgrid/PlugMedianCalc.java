package flink.application.smartgrid;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PlugMedianCalc extends Metrics implements FlatMapFunction<Tuple7<Long, String, String, String, Double, Integer, String>, Tuple5<String, String, Long, Double, String>> {

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
    public void flatMap(Tuple7<Long, String, String, String, Double, Integer, String> input, Collector<Tuple5<String, String, Long, Double, String>> out) {
        super.initialize(config);
        runMed();
        tsMap();

        int operation = input.getField(5);
        double value = input.getField(4);
        long timestamp = input.getField(0);
        String key = getKey(input);

        RunningMedianCalculator medianCalc = runningMedians.get(key);
        if (medianCalc == null) {
            medianCalc = new RunningMedianCalculator();
            runningMedians.put(key, medianCalc);
        }

        Long lastUpdatedTs = lastUpdatedTsMap.get(key);
        if (lastUpdatedTs == null) {
            lastUpdatedTs = 0l;
        }

        if (operation == 1) {
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTsMap.put(key, timestamp);
                out.collect(new Tuple5<>("plugMedianCalculator" ,key, timestamp, median, input.f6));
            }
        } else {
            medianCalc.remove(value);
        }
        super.calculateThroughput();
    }

    private String getKey(Tuple tuple) {
        return tuple.getField(1).toString() + ':' +
                tuple.getField(2).toString() + ':' +
                tuple.getField(3).toString();
    }
}
