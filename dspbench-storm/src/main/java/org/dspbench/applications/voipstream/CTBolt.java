package org.dspbench.applications.voipstream;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.Field;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;

/**
 * Per-user total call time
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTBolt extends AbstractFilterBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CTBolt.class);

    public CTBolt(String configPrefix) {
        super(configPrefix, Field.CALLTIME);
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
        boolean newCallee = input.getBooleanByField(Field.NEW_CALLEE);
        
        if (cdr.isCallEstablished() && newCallee) {
            String caller = input.getStringByField(Field.CALLING_NUM);
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            filter.add(caller, cdr.getCallDuration(), timestamp);
            double calltime = filter.estimateCount(caller, timestamp);

            LOG.debug(String.format("CallTime: %f", calltime));
            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(new Values(caller, timestamp, calltime, cdr));
        }
    }
}