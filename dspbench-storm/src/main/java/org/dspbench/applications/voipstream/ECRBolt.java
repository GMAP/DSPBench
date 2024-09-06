package org.dspbench.applications.voipstream;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.Field;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ECRBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ECRBolt.class);

    public ECRBolt(String configPrefix) {
        super(configPrefix, Field.RATE);
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
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(Field.RECORD);
        
        if (cdr.isCallEstablished()) {
            String caller  = cdr.getCallingNumber();
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);
            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(new Values(caller, timestamp, ecr, cdr));
        }
    }
}
