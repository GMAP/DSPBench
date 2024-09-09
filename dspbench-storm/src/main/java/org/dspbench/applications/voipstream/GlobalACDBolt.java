package org.dspbench.applications.voipstream;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.bolt.AbstractBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.util.config.Configuration;
import org.dspbench.util.math.VariableEWMA;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACDBolt.class);
    
    private VariableEWMA avgCallDuration;
    private double decayFactor;

    @Override
    public Fields getDefaultFields() {
        return new Fields(VoIPSTREAMConstants.Field.TIMESTAMP, VoIPSTREAMConstants.Field.AVERAGE);
    }

    @Override
    public void initialize() {
        decayFactor = config.getDouble(VoIPSTREAMConstants.Conf.ACD_DECAY_FACTOR, 86400); //86400s = 24h
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(VoIPSTREAMConstants.Field.RECORD);
        long timestamp = cdr.getAnswerTime().getMillis()/1000;

        avgCallDuration.add(cdr.getCallDuration());
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            emittedThroughput();
        }
        collector.emit(new Values(timestamp, avgCallDuration.getAverage()));
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }
}