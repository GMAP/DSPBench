package com.streamer.examples.sentimentanalysis;

import com.streamer.base.constants.BaseConstants;

/**
 *
 * @author mayconbordin
 */
public interface SentimentAnalysisConstants extends BaseConstants {
    String PREFIX = "sa";
    
    interface Config extends BaseConfig {
        String CLASSIFIER_THREADS       = "sa.classifier.threads";
        String CLASSIFIER_TYPE          = "sa.classifier.type";
        String LINGPIPE_CLASSIFIER_PATH = "sa.classifier.lingpipe.path";
        String BASIC_CLASSIFIER_PATH    = "sa.classifier.basic.path";
    }
    
    interface Component extends BaseComponent {
        String CLASSIFIER = "classifierBolt";
    }
    
    interface Field {
        String TWEET = "tweet";
        String ID = "id";
        String TEXT = "text";
        String TIMESTAMP = "timestamp";
        String SENTIMENT = "sentiment";
        String SCORE = "score";
    }
    
    interface Streams {
        String TWEETS = "tweetStream";
        String SENTIMENTS = "sentiments";
    }
}
