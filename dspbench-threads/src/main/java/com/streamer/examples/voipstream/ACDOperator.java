package com.streamer.examples.voipstream;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ACDOperator extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ACDOperator.class);

    private double avg;

    public ACDOperator() {
        super("acd");
    }

    public void process(Tuple input) {
        Source src = parseComponentId(input.getComponentName());
        
        if (src == Source.GACD) {
            avg = input.getDouble(Field.AVERAGE);
        } else {
            String number  = input.getString(Field.CALLING_NUM);
            long timestamp = input.getLong(Field.ANSWER_TIME);
            double rate    = input.contains(Field.RATE) ? input.getDouble(Field.RATE) : input.getDouble(Field.CALLTIME);


            String key = String.format("%s:%d", number, timestamp);

            if (map.containsKey(key)) {
                Entry e = map.get(key);
                e.set(src, rate);

                if (e.isFull()) {
                    // calculate the score for the ratio
                    double ratio = (e.get(Source.CT24)/e.get(Source.ECR24))/avg;
                    double score = score(thresholdMin, thresholdMax, ratio);
                    
                    //LOG.info(String.format("T1=%f; T2=%f; CT24=%f; ECR24=%f; AvgCallDur=%f; Ratio=%f; Score=%f",
                    //    thresholdMin, thresholdMax, e.get(Source.CT24), e.get(Source.ECR24), avg, ratio, score));

                    LOG.info(String.format("Caller: %s; AnswerTime: %d; Score: %f", number, timestamp, score));

                    emit(new Values(number, timestamp, score));
                    map.remove(key);
                } else {
                    LOG.warn(String.format("Inconsistent entry: source=%s; %s",
                            input.getComponentName(), e.toString()));
                }
            } else {
                Entry e = new Entry();
                e.set(src, rate);
                map.put(key, e);
            }
        }
    }
    
    @Override
    protected Source[] getFields() {
        return new Source[]{Source.CT24, Source.ECR24};
    }
}