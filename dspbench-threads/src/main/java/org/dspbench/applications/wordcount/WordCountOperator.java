package org.dspbench.applications.wordcount;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class WordCountOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountOperator.class);
    private Map<String, MutableLong> counts = new HashMap<String, MutableLong>();
    
    public void process(Tuple tuple) {
        String word = tuple.getString(WordCountConstants.Field.WORD);
        MutableLong count = counts.get(word);
        
        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();
                        
        emit(tuple, new Values(word, count.longValue()));
    }
    
}
