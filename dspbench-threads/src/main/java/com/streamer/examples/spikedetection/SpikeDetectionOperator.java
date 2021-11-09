package com.streamer.examples.spikedetection;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.spikedetection.SpikeDetectionConstants.Config;
import com.streamer.examples.spikedetection.SpikeDetectionConstants.Field;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionOperator extends BaseOperator {
    private double spikeThreshold;

    @Override
    public void initialize() {
        spikeThreshold = config.getDouble(Config.SPIKE_DETECTOR_THRESHOLD, 0.03);
    }
    
    @Override
    public void process(Tuple tuple) {
        String deviceID = tuple.getString(Field.DEVICE_ID);
        double movingAverageInstant = tuple.getDouble(Field.MOVING_AVG);
        double nextDouble = tuple.getDouble(Field.VALUE);
        
        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            emit(tuple, new Values(deviceID, movingAverageInstant, nextDouble));
        }
    }
}