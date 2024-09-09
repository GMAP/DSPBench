package org.dspbench.applications.trendingtopics;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.applications.trendingtopics.TrendingTopicsConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.json.simple.JSONObject;

/**
 *
 * @author mayconbordin
 */
public class TopicExtractorBolt extends AbstractBolt {
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
        Map tweet = (Map) input.getValueByField(TrendingTopicsConstants.Field.TWEET);
        JSONObject data = (JSONObject) tweet.get("data");
        String text = (String) data.get("text");
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                        emittedThroughput();
                    }
                    collector.emit(input, new Values(term));
                }
            }
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(TrendingTopicsConstants.Field.WORD);
    }
}
