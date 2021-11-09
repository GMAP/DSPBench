package com.streamer.examples.wordcount;

import com.streamer.core.Source;
import com.streamer.core.Values;
import java.util.Random;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SentenceGeneratorSource extends Source {
    private static final String[] sentences = new String[]{
        "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs", 
        "i am at two with nature"
    };
    
    private final Random rand = new Random();
    private long count = 0;
    
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public void nextTuple() {
        String sentence = sentences[rand.nextInt(sentences.length)];
        emit(new Values(sentence));
        count++;
    }
    
}
