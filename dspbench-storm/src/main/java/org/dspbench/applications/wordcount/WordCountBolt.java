package org.dspbench.applications.wordcount;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.MutableLong;
import org.dspbench.util.config.Configuration;
import java.util.HashMap;
import java.util.Map;

import org.dspbench.applications.wordcount.WordCountConstants;
import org.dspbench.bolt.AbstractBolt;

public class WordCountBolt extends AbstractBolt {
    private final Map<String, MutableLong> counts = new HashMap<>();

    @Override
    public Fields getDefaultFields() {
        return new Fields(WordCountConstants.Field.WORD, WordCountConstants.Field.COUNT);
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            recemitThroughput();
        }
        String word = input.getStringByField(WordCountConstants.Field.WORD);
        MutableLong count = counts.get(word);

        if (count == null) {
            count = new MutableLong(0);
            counts.put(word, count);
        }
        count.increment();

        collector.emit(input, new Values(word, count.get()));
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

}
