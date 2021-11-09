package com.streamer.examples.trendingtopics;

import com.streamer.base.constants.BaseConstants;

public interface TrendingTopicsConstants extends BaseConstants {
    String PREFIX = "tt";
    
    interface Component extends BaseComponent {
        String TOPIC_EXTRACTOR = "topicExtractorBolt";
        String COUNTER = "counterBolt";
        String INTERMEDIATE_RANKER = "intermediateRankerBolt";
        String TOTAL_RANKER = "totalRankerBolt";
    }
    
    interface Config extends BaseConfig {
        String TOPIC_EXTRACTOR_THREADS = "tt.topic_extractor.threads";
        String COUNTER_THREADS = "tt.counter.threads";
        String COUNTER_WINDOW = "tt.counter.window";
        String COUNTER_FREQ = "tt.counter.frequency";
        String IRANKER_THREADS = "tt.iranker.threads";
        String IRANKER_FREQ = "tt.iranker.frequency";
        String TRANKER_THREADS = "tt.tranker.threads";
        String TRANKER_FREQ = "tt.tranker.frequency";
        String TOPK = "tt.topk";
    }
    
    interface Field {
        String TWEET = "tweet";
        String WORD  = "word";
        String OBJ = "obj";
        String RANKINGS   = "rankings";
        String COUNT = "count";
        String WINDOW_LENGTH = "windowLength";
    }
    
    interface Streams {
        String TWEETS = "tweetStream";
        String TOPICS = "topicStream";
        String COUNTS = "countStream";
        String I_RANKINGS = "intermediateRankingStream";
        String T_RANKINGS = "totalRankingStream";
    }
}
