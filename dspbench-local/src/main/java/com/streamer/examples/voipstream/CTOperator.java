package com.streamer.examples.voipstream;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;
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
        boolean callEstablished = input.getBoolean(Field.CALL_ESTABLISHED);
        boolean newCallee = input.getBoolean(Field.NEW_CALLEE);
        
        if (callEstablished && newCallee) {
            String caller  = input.getString(Field.CALLING_NUM);
            long timestamp = input.getLong(Field.ANSWER_TIME);
            int callDuration = input.getInt(Field.CALL_DURATION);

            filter.add(caller, callDuration, timestamp);
            double calltime = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("Caller: %s; AnswerTime: %d; CallTime: %f", caller, timestamp, calltime));
            emit(new Values(caller, timestamp, calltime));
        }
    }
}