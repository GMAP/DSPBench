package com.streamer.examples.trendingtopics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TopicExtractorOperator extends BaseOperator {

    public void process(Tuple tuple) {
        Map tweet = (Map) tuple.getValue("tweet");
        String text = (String) tweet.get("text");
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    emit(tuple, new Values(term));
                }
            }
        }
    }
    
}
