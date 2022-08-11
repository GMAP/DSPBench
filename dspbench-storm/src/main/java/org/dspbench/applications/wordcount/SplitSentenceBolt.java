package org.dspbench.applications.wordcount;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.applications.wordcount.WordCountConstants.Field;
import org.dspbench.bolt.AbstractBolt;

public class SplitSentenceBolt extends AbstractBolt {
    private static final String splitregex = "\\W";
    
    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple input) {
        try {
            Thread.sleep(50);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
        String[] words = input.getString(0).split(splitregex);
        
        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word));
        }
        
        collector.ack(input);
    }
    
}
