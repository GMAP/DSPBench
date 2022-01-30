package org.dspbench.applications.voipstream;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Per-user received call rate.
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ECROperator extends AbstractFilterOperator {
    private static final Logger LOG = LoggerFactory.getLogger(ECROperator.class);

    public ECROperator() {
        super("ecr");
    }

    public ECROperator(String configPrefix) {
        super(configPrefix);
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean(VoIPSTREAMConstants.Field.CALL_ESTABLISHED);
        
        if (callEstablished) {
            String caller  = input.getString(VoIPSTREAMConstants.Field.CALLING_NUM);
            long timestamp = input.getLong(VoIPSTREAMConstants.Field.ANSWER_TIME);

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("Caller: %s; AnswerTime: %d; ECR: %f", caller, timestamp, ecr));
            emit(new Values(caller, timestamp, ecr));
        }
    }
}
