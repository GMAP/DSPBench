package com.streamer.examples.machineoutlier;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.base.task.BasicTask;
import static com.streamer.examples.machineoutlier.MachineOutlierConstants.*;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MachineOutlierTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlierTask.class);
    
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        scorerThreads         = config.getInt(Config.SCORER_THREADS, 1);
        anomalyScorerThreads  = config.getInt(Config.ANOMALY_SCORER_THREADS, 1);
        alertTriggerThreads   = config.getInt(Config.ALERT_TRIGGER_THREADS, 1);
    }
    
    public void initialize() {
        Stream readings = builder.createStream(Streams.READINGS,
                new Schema(Field.ID, Field.TIMESTAMP, Field.OBSERVATION));
        Stream scores = builder.createStream(Streams.SCORES,
                new Schema().keys(Field.ID).fields(Field.SCORE, Field.TIMESTAMP, Field.OBSERVATION));
        Stream anomalyScores = builder.createStream(Streams.ANOMALIES,
                new Schema(Field.ID, Field.ANOMALY_SCORE, Field.TIMESTAMP, Field.OBSERVATION, Field.SCORE));
        Stream alerts = builder.createStream(Streams.ALERTS,
                new Schema(Field.ID, Field.ANOMALY_SCORE, Field.TIMESTAMP, Field.IS_ABNORMAL, Field.OBSERVATION));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, readings);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.SCORER, new ObservationScoreOperator(), scorerThreads);
        builder.shuffle(Component.SCORER, readings);
        builder.publish(Component.SCORER, scores);
        
        builder.setOperator(Component.ANOMALY_SCORER, new SlidingWindowStreamAnomalyScoreOperator(), anomalyScorerThreads);
        //builder.setOperator(Component.ANOMALY_SCORER, new DataStreamAnomalyScoreOperator(), anomalyScorerThreads);
        builder.groupByKey(Component.ANOMALY_SCORER, scores);
        builder.publish(Component.ANOMALY_SCORER, anomalyScores);
        
        builder.setOperator(Component.ALERT_TRIGGER, new AlertTriggerOperator(), alertTriggerThreads);
        builder.shuffle(Component.ALERT_TRIGGER, anomalyScores);
        builder.publish(Component.ALERT_TRIGGER, alerts);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, alerts);
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
