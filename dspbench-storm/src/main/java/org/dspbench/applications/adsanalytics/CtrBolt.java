package org.dspbench.applications.adsanalytics;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

import org.dspbench.bolt.AbstractBolt;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CtrBolt extends AbstractBolt {
    private Map<String, Summary> summaries;

    @Override
    public Fields getDefaultFields() {
        return new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID, AdsAnalyticsConstants.Field.CTR);
    }

    @Override
    public void initialize() {
        summaries = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        AdEvent event = (AdEvent) input.getValueByField(AdsAnalyticsConstants.Field.EVENT);
        
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        Summary summary = summaries.get(key);
        
        // create summary if it don't exists
        if (summary == null) {
            summary = new Summary();
            summaries.put(key, summary);
        }
        
        // update summary
        if (input.getSourceStreamId().equals(AdsAnalyticsConstants.Stream.CLICKS)) {
            summary.clicks++;
        } else {
            summary.impressions++;
        }
        
        // calculate ctr
        double ctr = (double)summary.clicks / (double)summary.impressions;
        
        collector.emit(input, new Values(event.getQueryId(), event.getAdID(), ctr));
        collector.ack(input);
    }
    
    private static class Summary {
        public long impressions = 0;
        public long clicks = 0;
    }
}
