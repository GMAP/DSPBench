package org.dspbench.applications.trendingtopics;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TopicExtractorOperator extends BaseOperator {

    public void process(Tuple tuple) {
        String text = tuple.getString(TrendingTopicsConstants.Field.TEXT);
        
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
