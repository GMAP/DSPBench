package com.streamer.examples.spamfilter;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.spamfilter.SpamFilterConstants.Field;
import com.streamer.examples.spamfilter.SpamFilterConstants.Streams;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TokenizerOperator extends BaseOperator {
    private static final String splitregex = "\\W";
    private static final Pattern wordregex = Pattern.compile("\\w+");


    @Override
    public void process(Tuple input) {
        String content = input.getString(Field.MESSAGE);

        if (input.getStreamId().equals(Streams.T_EMAILS)) {
            boolean isSpam = input.getBoolean(Field.IS_SPAM);
            
            Map<String, MutableInt> words = tokenize(content);
            int spamTotal = 0, hamTotal = 0;

            for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
                String word = entry.getKey();
                int count = entry.getValue().toInteger();
                
                if (isSpam) {
                    spamTotal += count;
                } else {
                    hamTotal += count;
                }
                
                emit(Streams.T_TOKENS, input, new Values(word, count, isSpam));
            }
            
            emit(Streams.T_SUMS, input, new Values(spamTotal, hamTotal));
        }
        
        else if (input.getStreamId().equals(Streams.EMAILS)) {
            String emailId = input.getString(Field.ID);
            
            Map<String, MutableInt> words = tokenize(content);
            
            for (Map.Entry<String, MutableInt> entry : words.entrySet()) {
                emit(Streams.TOKENS, input, new Values(emailId, entry.getKey(), words.size()));
            }
        }
    }
    
    private Map<String, MutableInt> tokenize(String content) {
        String[] tokens = content.split(splitregex);
        Map<String, MutableInt> words = new HashMap<String, MutableInt>();

        for (String token : tokens) {
            String word = token.toLowerCase();
            Matcher m = wordregex.matcher(word);

            if (m.matches()) {
                MutableInt count = words.get(word);
                if (count == null) {
                    words.put(word, new MutableInt(1));
                } else {
                    count.increment();
                }
            }
        }
        
        return words;
    }
    
}
