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
public class RCROperator extends AbstractFilterOperator {
    private static final Logger LOG = LoggerFactory.getLogger(RCROperator.class);

    public RCROperator() {
        super("rcr");
    }

    public void process(Tuple input) {
        boolean callEstablished = input.getBoolean(Field.CALL_ESTABLISHED);
        
        if (callEstablished) {
            long timestamp = input.getLong(Field.ANSWER_TIME);
            
            if (input.getStreamId().equals(Streams.VARIATIONS)) {
                String callee = input.getString(Field.CALLED_NUM);
                filter.add(callee, 1, timestamp);
            }

            else if (input.getStreamId().equals(Streams.VARIATIONS_BACKUP)) {
                String caller = input.getString(Field.CALLING_NUM);
                double rcr = filter.estimateCount(caller, timestamp);

                LOG.info(String.format("Caller: %s; AnswerTime: %d; RCR: %f", caller, timestamp, rcr));
                emit(new Values(caller, timestamp, rcr));
            }
        }
    }
}
