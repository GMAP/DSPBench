package org.dspbench.applications.smartgrid;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.utils.math.RunningMedianCalculator;
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
        int operation  = tuple.getInt(SmartGridConstants.Field.SLIDING_WINDOW_ACTION);
        double value   = tuple.getDouble(SmartGridConstants.Field.VALUE);
        long timestamp = tuple.getLong(SmartGridConstants.Field.TIMESTAMP);

        if (operation == SmartGridConstants.SlidingWindowAction.ADD) {
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
