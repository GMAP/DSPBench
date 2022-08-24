package org.dspbench.applications.machineoutlier;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MachineOutlierTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlierTopology.class);
    
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;

    public MachineOutlierTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        scorerThreads        = config.getInt(MachineOutlierConstants.Conf.SCORER_THREADS, 1);
        anomalyScorerThreads = config.getInt(MachineOutlierConstants.Conf.ANOMALY_SCORER_THREADS, 1);
        alertTriggerThreads  = config.getInt(MachineOutlierConstants.Conf.ALERT_TRIGGER_THREADS, 1);
    }
    
    @Override
    public StormTopology buildTopology() {
        spout.setFields(new Fields(MachineOutlierConstants.Field.ID, MachineOutlierConstants.Field.TIMESTAMP, MachineOutlierConstants.Field.OBSERVATION, MachineOutlierConstants.Field.INITTIME));
        
        builder.setSpout(MachineOutlierConstants.Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(MachineOutlierConstants.Component.SCORER, new ObservationScoreBolt(), scorerThreads)
               .shuffleGrouping(MachineOutlierConstants.Component.SPOUT);
        
        builder.setBolt(MachineOutlierConstants.Component.ANOMALY_SCORER, new SlidingWindowStreamAnomalyScoreBolt(), anomalyScorerThreads)
               .fieldsGrouping(MachineOutlierConstants.Component.SCORER, new Fields(MachineOutlierConstants.Field.ID));

        builder.setBolt(MachineOutlierConstants.Component.ALERT_TRIGGER, new AlertTriggerBolt(), alertTriggerThreads)
               .shuffleGrouping(MachineOutlierConstants.Component.ANOMALY_SCORER);
        
        builder.setBolt(MachineOutlierConstants.Component.SINK, sink, sinkThreads)
               .shuffleGrouping(MachineOutlierConstants.Component.ALERT_TRIGGER);

        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return MachineOutlierConstants.PREFIX;
    }
}
