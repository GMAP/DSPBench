package org.dspbench.applications.smartgrid;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.math.RunningMedianCalculator;
import org.dspbench.applications.smartgrid.SmartGridConstants.Field;
import org.dspbench.applications.smartgrid.SmartGridConstants.SlidingWindowAction;

/**
 *
 * @author mayconbordin
 */
public class GlobalMedianCalculatorBolt extends AbstractBolt {
    private RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;
    
    @Override
    public void initialize() {
        medianCalc = new RunningMedianCalculator();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.TIMESTAMP, Field.GLOBAL_MEDIAN_LOAD, Field.INITTIME);
    }

    @Override
    public void execute(Tuple tuple) {
        int operation  = tuple.getIntegerByField(Field.SLIDING_WINDOW_ACTION);
        double value   = tuple.getDoubleByField(Field.VALUE);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        
        if (operation == SlidingWindowAction.ADD){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                collector.emit(new Values(timestamp, median, tuple.getStringByField(Field.INITTIME)));
            }
        } else {
            medianCalc.remove(value);
        }
    }
    
}
