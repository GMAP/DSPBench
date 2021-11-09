package com.streamer.base.source.generator;

import com.streamer.core.Values;
import com.streamer.utils.Configuration;
import java.util.Random;

public class RandomSentenceGenerator extends Generator {
    private static final String[] sentences = new String[]{
        "the cow jumped over the moon", "an apple a day keeps the doctor away",
        "four score and seven years ago", "snow white and the seven dwarfs",
        "i am at two with nature"
    };
    
    private Random rand;
    private int count = 0;

    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        rand = new Random();
    }
    
    @Override
    public Values generate() {
        Values values = new Values(sentences[rand.nextInt(sentences.length)]);
        values.setId(count++);
        return values;
    }
    
}