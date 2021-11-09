package com.streamer.examples.trendingtopics;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import static com.streamer.examples.trendingtopics.TrendingTopicsConstants.*;
import com.streamer.base.task.BasicTask;
import com.streamer.utils.Configuration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrendingTopicsTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(TrendingTopicsTask.class);
    
    private int topicExtractorThreads;
    private int counterThreads;
    private int iRankerThreads;
    
    private int counterFrequency;
    private int iRankerFrequency;
    private int tRankerFrequnecy;
    
    @Override
    public void setConfiguration(Configuration config) {        
        super.setConfiguration(config);
        
        topicExtractorThreads = config.getInt(Config.TOPIC_EXTRACTOR_THREADS, 1);
        counterThreads        = config.getInt(Config.COUNTER_THREADS, 1);
        iRankerThreads        = config.getInt(Config.IRANKER_THREADS, 1);
        counterFrequency      = config.getInt(Config.COUNTER_FREQ, 60);
        iRankerFrequency      = config.getInt(Config.IRANKER_FREQ, 2);
        tRankerFrequnecy      = config.getInt(Config.TRANKER_FREQ, 2);
    }

    public void initialize() {
        Stream tweets = builder.createStream(Streams.TWEETS, new Schema(Field.TWEET));
        Stream topics = builder.createStream(Streams.TOPICS, new Schema().keys(Field.WORD));
        Stream counts = builder.createStream(Streams.COUNTS, new Schema().keys(Field.WORD).fields(Field.COUNT, Field.WINDOW_LENGTH));
        Stream iRankings = builder.createStream(Streams.I_RANKINGS, new Schema(Field.RANKINGS));
        Stream tRankings = builder.createStream(Streams.T_RANKINGS, new Schema(Field.RANKINGS));

        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, tweets);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.TOPIC_EXTRACTOR, new TopicExtractorOperator(), topicExtractorThreads);
        builder.shuffle(Component.TOPIC_EXTRACTOR, tweets);
        builder.publish(Component.TOPIC_EXTRACTOR, topics);

        builder.setOperator(Component.COUNTER, new RollingCounterOperator(), counterThreads);
        builder.groupByKey(Component.COUNTER, topics);
        builder.publish(Component.COUNTER, counts);
        builder.setTimer(Component.COUNTER, counterFrequency, TimeUnit.SECONDS);

        builder.setOperator(Component.INTERMEDIATE_RANKER, new IntermediateRankerOperator(), iRankerThreads);
        builder.groupByKey(Component.INTERMEDIATE_RANKER, counts);
        builder.publish(Component.INTERMEDIATE_RANKER, iRankings);
        builder.setTimer(Component.INTERMEDIATE_RANKER, iRankerFrequency, TimeUnit.SECONDS);
        
        builder.setOperator(Component.TOTAL_RANKER, new TotalRankerOperator(), 1);
        builder.bcast(Component.TOTAL_RANKER, iRankings);
        builder.publish(Component.TOTAL_RANKER, tRankings);
        builder.setTimer(Component.TOTAL_RANKER, tRankerFrequnecy, TimeUnit.SECONDS);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, tRankings);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
