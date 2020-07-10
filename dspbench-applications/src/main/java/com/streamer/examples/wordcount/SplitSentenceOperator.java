package com.streamer.examples.wordcount;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.wordcount.WordCountConstants.Field;
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
        String[] words = tuple.getString(Field.TEXT).split(splitregex);
        
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                emit(tuple, new Values(word.toUpperCase().trim()));
        }
    }
}
