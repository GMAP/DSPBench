package org.dspbench.applications.spikedetection;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.applications.spikedetection.SpikeDetectionConstants.Conf;
import org.dspbench.applications.spikedetection.SpikeDetectionConstants.Field;
import org.dspbench.applications.wordcount.WordCountConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;

/**
 * Emits a tuple if the current value surpasses a pre-defined threshold.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionBolt extends AbstractBolt {
    private double spikeThreshold;

    @Override
    public void initialize() {
        spikeThreshold = config.getDouble(Conf.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        String deviceID = input.getStringByField(Field.DEVICE_ID);
        double movingAverageInstant = input.getDoubleByField(Field.MOVING_AVG);
        double nextDouble = input.getDoubleByField(Field.VALUE);
        
        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(input, new Values(deviceID, movingAverageInstant, nextDouble, "spike detected"));
        }
        
        collector.ack(input);
    }
    
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE, Field.MESSAGE);
    }
}