package org.dspbench.applications.spikedetection;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Detects spikes in values emitted from sensors.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class SpikeDetectionTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetectionTopology.class);
    
    private int movingAverageThreads;
    private int spikeDetectorThreads;

    public SpikeDetectionTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        movingAverageThreads = config.getInt(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInt(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(SpikeDetectionConstants.Field.DEVICE_ID, SpikeDetectionConstants.Field.TIMESTAMP, SpikeDetectionConstants.Field.VALUE));
        
        builder.setSpout(SpikeDetectionConstants.Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(SpikeDetectionConstants.Component.MOVING_AVERAGE, new MovingAverageBolt(), movingAverageThreads)
               .fieldsGrouping(SpikeDetectionConstants.Component.SPOUT, new Fields(SpikeDetectionConstants.Field.DEVICE_ID));
        
        builder.setBolt(SpikeDetectionConstants.Component.SPIKE_DETECTOR, new SpikeDetectionBolt(), spikeDetectorThreads)
               .shuffleGrouping(SpikeDetectionConstants.Component.MOVING_AVERAGE);
        
        builder.setBolt(SpikeDetectionConstants.Component.SINK, sink, sinkThreads)
               .shuffleGrouping(SpikeDetectionConstants.Component.SPIKE_DETECTOR);
        
        return builder.createTopology();
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
