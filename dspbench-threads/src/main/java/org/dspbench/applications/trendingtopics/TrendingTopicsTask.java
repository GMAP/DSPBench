package org.dspbench.applications.trendingtopics;

import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.base.task.BasicTask;
import org.dspbench.utils.Configuration;
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
        
        topicExtractorThreads = config.getInt(TrendingTopicsConstants.Config.TOPIC_EXTRACTOR_THREADS, 1);
        counterThreads        = config.getInt(TrendingTopicsConstants.Config.COUNTER_THREADS, 1);
        iRankerThreads        = config.getInt(TrendingTopicsConstants.Config.IRANKER_THREADS, 1);
        counterFrequency      = config.getInt(TrendingTopicsConstants.Config.COUNTER_FREQ, 60);
        iRankerFrequency      = config.getInt(TrendingTopicsConstants.Config.IRANKER_FREQ, 2);
        tRankerFrequnecy      = config.getInt(TrendingTopicsConstants.Config.TRANKER_FREQ, 2);
    }

    public void initialize() {
        Stream tweets = builder.createStream(TrendingTopicsConstants.Streams.TWEETS, new Schema(TrendingTopicsConstants.Field.ID, TrendingTopicsConstants.Field.TEXT, TrendingTopicsConstants.Field.TIMESTAMP));
        Stream topics = builder.createStream(TrendingTopicsConstants.Streams.TOPICS, new Schema().keys(TrendingTopicsConstants.Field.WORD));
        Stream counts = builder.createStream(TrendingTopicsConstants.Streams.COUNTS, new Schema().keys(TrendingTopicsConstants.Field.WORD).fields(TrendingTopicsConstants.Field.COUNT, TrendingTopicsConstants.Field.WINDOW_LENGTH));
        Stream iRankings = builder.createStream(TrendingTopicsConstants.Streams.I_RANKINGS, new Schema(TrendingTopicsConstants.Field.RANKINGS));
        Stream tRankings = builder.createStream(TrendingTopicsConstants.Streams.T_RANKINGS, new Schema(TrendingTopicsConstants.Field.RANKINGS));

        builder.setSource(TrendingTopicsConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(TrendingTopicsConstants.Component.SOURCE, tweets);
        builder.setTupleRate(TrendingTopicsConstants.Component.SOURCE, sourceRate);
        
        builder.setOperator(TrendingTopicsConstants.Component.TOPIC_EXTRACTOR, new TopicExtractorOperator(), topicExtractorThreads);
        builder.shuffle(TrendingTopicsConstants.Component.TOPIC_EXTRACTOR, tweets);
        builder.publish(TrendingTopicsConstants.Component.TOPIC_EXTRACTOR, topics);

        builder.setOperator(TrendingTopicsConstants.Component.COUNTER, new RollingCounterOperator(), counterThreads);
        builder.groupByKey(TrendingTopicsConstants.Component.COUNTER, topics);
        builder.publish(TrendingTopicsConstants.Component.COUNTER, counts);
        builder.setTimer(TrendingTopicsConstants.Component.COUNTER, counterFrequency, TimeUnit.SECONDS);

        builder.setOperator(TrendingTopicsConstants.Component.INTERMEDIATE_RANKER, new IntermediateRankerOperator(), iRankerThreads);
        builder.groupByKey(TrendingTopicsConstants.Component.INTERMEDIATE_RANKER, counts);
        builder.publish(TrendingTopicsConstants.Component.INTERMEDIATE_RANKER, iRankings);
        builder.setTimer(TrendingTopicsConstants.Component.INTERMEDIATE_RANKER, iRankerFrequency, TimeUnit.SECONDS);
        
        builder.setOperator(TrendingTopicsConstants.Component.TOTAL_RANKER, new TotalRankerOperator(), 1);
        builder.bcast(TrendingTopicsConstants.Component.TOTAL_RANKER, iRankings);
        builder.publish(TrendingTopicsConstants.Component.TOTAL_RANKER, tRankings);
        builder.setTimer(TrendingTopicsConstants.Component.TOTAL_RANKER, tRankerFrequnecy, TimeUnit.SECONDS);
        
        builder.setOperator(TrendingTopicsConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(TrendingTopicsConstants.Component.SINK, tRankings);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return TrendingTopicsConstants.PREFIX;
    }
}
