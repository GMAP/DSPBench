package flink.application.YSB;

import java.util.HashMap;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.source.YSB_Event;
import org.apache.flink.configuration.Configuration;

public class Joiner extends RichFlatMapFunction<YSB_Event, Joined_Event> {
    private static final Logger LOG = LoggerFactory.getLogger(Joiner.class);

    private HashMap<String, String> campaignLookup;
    Metrics metric = new Metrics();

    Joiner(Configuration config, HashMap<String, String> campaignLookup) {
        this.campaignLookup = campaignLookup;
        metric.initialize(config);
    }

    @Override
    public void flatMap(YSB_Event value, Collector<Joined_Event> out) throws Exception {
        String ad_id = value.ad_id;
        long ts = value.timestamp;
        String campaign_id = campaignLookup.get(ad_id);
        metric.incReceived(this.getClass().getSimpleName());
        if (campaign_id != null) {
            metric.incEmitted(this.getClass().getSimpleName());
            out.collect(new Joined_Event(campaign_id, ad_id, ts));
        }
    }
    
}
