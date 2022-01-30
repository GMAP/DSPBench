package org.dspbench.applications.voipstream;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-user total call time
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CTOperator extends AbstractFilterOperator {
    private static final Logger LOG = LoggerFactory.getLogger(CTOperator.class);

    public CTOperator() {
        super("ct24");
    }

    public CTOperator(String configPrefix) {
        super(configPrefix);
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean(VoIPSTREAMConstants.Field.CALL_ESTABLISHED);
        boolean newCallee = input.getBoolean(VoIPSTREAMConstants.Field.NEW_CALLEE);
        
        if (callEstablished && newCallee) {
            String caller  = input.getString(VoIPSTREAMConstants.Field.CALLING_NUM);
            long timestamp = input.getLong(VoIPSTREAMConstants.Field.ANSWER_TIME);
            int callDuration = input.getInt(VoIPSTREAMConstants.Field.CALL_DURATION);

            filter.add(caller, callDuration, timestamp);
            double calltime = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("Caller: %s; AnswerTime: %d; CallTime: %f", caller, timestamp, calltime));
            emit(new Values(caller, timestamp, calltime));
        }
    }
}