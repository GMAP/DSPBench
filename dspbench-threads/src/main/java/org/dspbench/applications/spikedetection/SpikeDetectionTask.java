package org.dspbench.applications.spikedetection;

import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.base.task.BasicTask;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects spikes in values emitted from sensors.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionTask.class);
    
    private int movingAvgerageThreads;
    private int spikeDetectorThreads;
    
    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        movingAvgerageThreads = config.getInt(SpikeDetectionConstants.Config.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads  = config.getInt(SpikeDetectionConstants.Config.SPIKE_DETECTOR_THREADS, 1);
    }

    public void initialize() {
        Stream sensors = builder.createStream(SpikeDetectionConstants.Streams.SENSORS, new Schema().keys(SpikeDetectionConstants.Field.DEVICE_ID).fields(SpikeDetectionConstants.Field.TIMESTAMP, SpikeDetectionConstants.Field.VALUE));
        Stream average = builder.createStream(SpikeDetectionConstants.Streams.AVERAGES, new Schema().keys(SpikeDetectionConstants.Field.DEVICE_ID).fields(SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE));
        Stream spikes  = builder.createStream(SpikeDetectionConstants.Streams.SPIKES, new Schema().keys(SpikeDetectionConstants.Field.DEVICE_ID).fields(SpikeDetectionConstants.Field.MOVING_AVG, SpikeDetectionConstants.Field.VALUE));
        
        builder.setSource(SpikeDetectionConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(SpikeDetectionConstants.Component.SOURCE, sensors);
        
        builder.setOperator(SpikeDetectionConstants.Component.MOVING_AVERAGE, new MovingAverageOperator(), movingAvgerageThreads);
        builder.groupByKey(SpikeDetectionConstants.Component.MOVING_AVERAGE, sensors);
        builder.publish(SpikeDetectionConstants.Component.MOVING_AVERAGE, average);
        
        builder.setOperator(SpikeDetectionConstants.Component.SPIKE_DETECTOR, new SpikeDetectionOperator(), spikeDetectorThreads);
        builder.shuffle(SpikeDetectionConstants.Component.SPIKE_DETECTOR, average);
        builder.publish(SpikeDetectionConstants.Component.SPIKE_DETECTOR, spikes);
        
        builder.setOperator(SpikeDetectionConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(SpikeDetectionConstants.Component.SINK, spikes);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return SpikeDetectionConstants.PREFIX;
    }
}
