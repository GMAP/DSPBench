package com.streamer.examples.wordcount;

import com.streamer.base.constants.BaseConstants;

/**
 *
 * @author mayconbordin
 */
public interface WordCountConstants extends BaseConstants {
    String PREFIX = "wc";
    
    interface Field {
        String TEXT  = "text";
        String WORD  = "word";
        String COUNT = "count";
    }
    
    interface Config extends BaseConfig {
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
    }
    
    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER  = "wordCount";
    }
    
    interface Streams {
        String SENTENCES = "sentenceStream";
        String WORDS = "wordStream";
        String COUNTS = "countStream";
    }
}
