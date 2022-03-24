package org.dspbench.applications.wordcount;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SplitSentenceOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceOperator.class);
    
    private static final String splitregex = "\\W";
    
    public void process(Tuple tuple) {
        String[] words = tuple.getString(WordCountConstants.Field.TEXT).split(splitregex);
        
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                emit(tuple, new Values(word.toUpperCase().trim()));
        }
    }
}
