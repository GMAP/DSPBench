package com.streamer.examples.smartgrid;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.smartgrid.SmartGridConstants.*;
import com.streamer.examples.utils.math.RunningMedianCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalMedianCalculatorOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalMedianCalculatorOperator.class);

    private RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;

    @Override
    public void initialize() {
        medianCalc = new RunningMedianCalculator();
    }

    public void process(Tuple tuple) {
        int operation  = tuple.getInt(Field.SLIDING_WINDOW_ACTION);
        double value   = tuple.getDouble(Field.VALUE);
        long timestamp = tuple.getLong(Field.TIMESTAMP);

        if (operation == SlidingWindowAction.ADD) {
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                emit(new Values(timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }
}
