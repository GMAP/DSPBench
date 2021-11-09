package com.streamer.examples.voipstream;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;
import java.util.Date;
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
        boolean callEstablished = input.getBoolean(Field.CALL_ESTABLISHED);
        
        if (callEstablished) {
            String caller  = input.getString(Field.CALLING_NUM);
            long timestamp = input.getLong(Field.ANSWER_TIME);

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);

            LOG.info(String.format("Caller: %s; AnswerTime: %d; ECR: %f", caller, timestamp, ecr));
            emit(new Values(caller, timestamp, ecr));
        }
    }
}
