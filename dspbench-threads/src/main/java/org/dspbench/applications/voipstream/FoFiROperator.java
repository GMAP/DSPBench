package org.dspbench.applications.voipstream;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class FoFiROperator extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(FoFiROperator.class);

    public FoFiROperator() {
        super("fofir");
    }

    public void process(Tuple input) {
        String number  = input.getString(Field.CALLING_NUM);
        long timestamp = input.getLong(Field.ANSWER_TIME);
        double rate = input.getDouble(Field.RATE);
        
        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId(input.getComponentName());
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);
            
            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.ECR)/e.get(Source.RCR));
                double score = score(thresholdMin, thresholdMax, ratio);
                
                //LOG.info(String.format("T1=%f; T2=%f; ECR=%f; RCR=%f; Ratio=%f; Score=%f",
                //        thresholdMin, thresholdMax, e.get(Source.ECR), e.get(Source.RCR), ratio, score));

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

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.RCR, Source.ECR};
    }
}