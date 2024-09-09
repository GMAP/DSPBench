package org.dspbench.applications.voipstream;

import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.util.config.Configuration;
import org.apache.log4j.Logger;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class URLBolt extends AbstractScoreBolt {
    private static final Logger LOG = Logger.getLogger(URLBolt.class);
    
    public URLBolt() {
        super("url");
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
        CallDetailRecord cdr = (CallDetailRecord) input.getValue(3);
        String number  = input.getString(0);
        long timestamp = input.getLong(1);
        double rate    = input.getDouble(2);
        
        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId(input.getSourceComponent());
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);
            
            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.ENCR)/e.get(Source.ECR));
                double score = score(thresholdMin, thresholdMax, ratio);
                
                LOG.debug(String.format("T1=%f; T2=%f; ENCR=%f; ECR=%f; Ratio=%f; Score=%f", 
                        thresholdMin, thresholdMax, e.get(Source.ENCR), e.get(Source.ECR), ratio, score));
                
                if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                    emittedThroughput();
                }
                collector.emit(new Values(number, timestamp, score, cdr));
                map.remove(key);
            } else {
                LOG.warn(String.format("Inconsistent entry: source=%s; %s",
                        input.getSourceComponent(), e.toString()));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }
    }

    @Override
    protected Source[] getFields() {
        return new Source[]{Source.ENCR, Source.ECR};
    }
}