package com.streamer.examples.voipstream;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.utils.Configuration;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ScoreOperator extends AbstractScoreBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ScoreOperator.class);

    private double[] weights;

    public ScoreOperator() {
        super(null);
    }

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
                
        // parameters
        double fofirWeight = config.getDouble(Config.FOFIR_WEIGHT);
        double urlWeight   = config.getDouble(Config.URL_WEIGHT);
        double acdWeight   = config.getDouble(Config.ACD_WEIGHT);
        
        weights = new double[3];
        weights[0] = fofirWeight;
        weights[1] = urlWeight;
        weights[2] = acdWeight;
    }

    public void process(Tuple input) {
        Source src     = parseComponentId(input.getComponentName());
        String caller  = input.getString(Field.CALLING_NUM);
        long timestamp = input.getLong(Field.ANSWER_TIME);
        double score   = input.getDouble(Field.SCORE);
        String key     = String.format("%s:%d", caller, timestamp);
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, score);

            LOG.info(String.format("Caller: %s; AnswerTime: %d", caller, timestamp));
            LOG.info("SCORER_BOLT = " + key + " || VALUES = " + e.toString());
            
            if (e.isFull()) {
                double mainScore = sum(e.getValues(), weights);
                
                LOG.info(String.format("Score=%f; Scores=%s", mainScore, Arrays.toString(e.getValues())));
                
                emit(new Values(caller, timestamp, mainScore));
            }
        } else {
            Entry e = new Entry();
            e.set(src, score);
            map.put(key, e);
        }
    }
    
    /**
     * Computes weighted sum of a given sequence. 
     * @param data data array
     * @param weights weights
     * @return weighted sum of the data 
     */
    private static double sum(double[] data, double[] weights) {
        double sum = 0.0;

        for (int i=0; i<data.length; i++) {
            sum += (data[i] * weights[i]);
        }
        
        return sum;
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.FOFIR, Source.URL, Source.ACD};
    }
}
