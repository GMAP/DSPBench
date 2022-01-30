package org.dspbench.applications.voipstream;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-user new callee rate
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ENCROperator extends AbstractFilterOperator {
    private static final Logger LOG = LoggerFactory.getLogger(ENCROperator.class);

    public ENCROperator() {
        super("encr");
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean(Field.CALL_ESTABLISHED);
        boolean newCallee = input.getBoolean(Field.NEW_CALLEE);
        
        if (callEstablished && newCallee) {
            String caller = input.getString(Field.CALLING_NUM);
            long timestamp = input.getLong(Field.ANSWER_TIME);

            filter.add(caller, 1, timestamp);
            double rate = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("Caller: %s; AnswerTime: %d; Rate: %f", caller, timestamp, rate));
            emit(new Values(caller, timestamp, rate));
        }
    }
}