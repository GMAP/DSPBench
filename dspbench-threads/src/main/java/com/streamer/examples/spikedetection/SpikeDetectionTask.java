package com.streamer.examples.spikedetection;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.base.task.AbstractTask;
import com.streamer.base.task.BasicTask;
import static com.streamer.examples.spikedetection.SpikeDetectionConstants.*;
import com.streamer.utils.Configuration;
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
        
        movingAvgerageThreads = config.getInt(Config.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads  = config.getInt(Config.SPIKE_DETECTOR_THREADS, 1);
    }

    public void initialize() {
        Stream sensors = builder.createStream(Streams.SENSORS, new Schema().keys(Field.DEVICE_ID).fields(Field.TIMESTAMP, Field.VALUE));
        Stream average = builder.createStream(Streams.AVERAGES, new Schema().keys(Field.DEVICE_ID).fields(Field.MOVING_AVG, Field.VALUE));
        Stream spikes  = builder.createStream(Streams.SPIKES, new Schema().keys(Field.DEVICE_ID).fields(Field.MOVING_AVG, Field.VALUE));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, sensors);
        
        builder.setOperator(Component.MOVING_AVERAGE, new MovingAverageOperator(), movingAvgerageThreads);
        builder.groupByKey(Component.MOVING_AVERAGE, sensors);
        builder.publish(Component.MOVING_AVERAGE, average);
        
        builder.setOperator(Component.SPIKE_DETECTOR, new SpikeDetectionOperator(), spikeDetectorThreads);
        builder.shuffle(Component.SPIKE_DETECTOR, average);
        builder.publish(Component.SPIKE_DETECTOR, spikes);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, spikes);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
