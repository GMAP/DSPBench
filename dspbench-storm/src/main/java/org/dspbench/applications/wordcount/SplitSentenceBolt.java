package org.dspbench.applications.wordcount;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang3.StringUtils;
import org.dspbench.applications.wordcount.WordCountConstants.Field;
import org.dspbench.bolt.AbstractBolt;

import java.time.Instant;

public class SplitSentenceBolt extends AbstractBolt {
    private static final String splitregex = "\\W";

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD, WordCountConstants.Field.INITTIME);
    }

    @Override
    public void execute(Tuple input) {
        String time = super.getUnixTime();
        String[] words = input.getString(0).split(splitregex);

        for (String word : words) {
            if (!StringUtils.isBlank(word))
                collector.emit(input, new Values(word, time));
        }

        collector.ack(input);
    }

}
