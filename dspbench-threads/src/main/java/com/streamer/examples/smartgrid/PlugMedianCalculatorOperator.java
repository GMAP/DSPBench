package com.streamer.examples.smartgrid;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.utils.math.RunningMedianCalculator;
import com.streamer.examples.smartgrid.SmartGridConstants.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PlugMedianCalculatorOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalMedianCalculatorOperator.class);

    private Map<String, RunningMedianCalculator> runningMedians;
    private Map<String, Long> lastUpdatedTsMap;

    @Override
    public void initialize() {
        runningMedians = new HashMap<>();
        lastUpdatedTsMap = new HashMap<>();
    }

    public void process(Tuple tuple) {
        int operation  = tuple.getInt(Field.SLIDING_WINDOW_ACTION);
        double value   = tuple.getDouble(Field.VALUE);
        long timestamp = tuple.getLong(Field.TIMESTAMP);
        String key     = getKey(tuple);

        RunningMedianCalculator medianCalc = runningMedians.get(key);
        if (medianCalc == null) {
            medianCalc =  new RunningMedianCalculator();
            runningMedians.put(key, medianCalc);
        }

        Long lastUpdatedTs = lastUpdatedTsMap.get(key);
        if (lastUpdatedTs == null) {
            lastUpdatedTs = 0l;
        }

        if (operation == SlidingWindowAction.ADD){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTsMap.put(key, timestamp);
                emit(new Values(key, timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }

    private String getKey(Tuple tuple) {
        return tuple.getString(Field.HOUSE_ID) + ':' +
                tuple.getString(Field.HOUSEHOLD_ID) + ':' +
                tuple.getString(Field.PLUG_ID);
    }
}