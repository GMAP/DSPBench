package org.dspbench.applications.wordcount;

import org.dspbench.constants.BaseConstants;

public interface WordCountConstants extends BaseConstants {
    String PREFIX = "wc";
    
    interface Field {
        String TEXT  = "text";
        String WORD  = "word";
        String COUNT = "count";
        String INITTIME = "inittime";
    }
    
    interface Conf extends BaseConf {
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
    }
    
    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER  = "wordCount";
    }
}
