package org.dspbench.applications.wordcount;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import java.util.HashMap;
import java.util.Map;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.constants.WordCountConstants.Field;

public class WordCountBolt extends AbstractBolt {
    private final Map<String, MutableLong> counts = new HashMap<>();

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, Field.COUNT);
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField(Field.WORD);
        MutableLong count = counts.get(word);
        
        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();
        
        collector.emit(input, new Values(word, count.get()));
        collector.ack(input);
    }
    
}
