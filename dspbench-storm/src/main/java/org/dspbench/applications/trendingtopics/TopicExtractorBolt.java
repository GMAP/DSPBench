package org.dspbench.applications.trendingtopics;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.constants.TrendingTopicsConstants.Field;

/**
 *
 * @author mayconbordin
 */
public class TopicExtractorBolt extends AbstractBolt {
    @Override
    public void execute(Tuple input) {
        Map tweet = (Map) input.getValueByField(Field.TWEET);
        String text = (String) tweet.get("text");
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    collector.emit(input, new Values(term));
                }
            }
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }
}
