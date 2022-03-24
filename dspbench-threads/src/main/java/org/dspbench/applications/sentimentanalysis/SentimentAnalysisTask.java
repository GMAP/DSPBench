package org.dspbench.applications.sentimentanalysis;

import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.base.task.BasicTask;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the elements and forms a Topology to find the most happiest state
 * by analyzing and processing Tweets.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class SentimentAnalysisTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysisTask.class);
    
    private int classifierThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        classifierThreads = config.getInt(SentimentAnalysisConstants.Config.CLASSIFIER_THREADS, 1);
    }
    
    public void initialize() {
        Stream tweets = builder.createStream(SentimentAnalysisConstants.Streams.TWEETS, new Schema(SentimentAnalysisConstants.Field.ID, SentimentAnalysisConstants.Field.TEXT, SentimentAnalysisConstants.Field.TIMESTAMP));
        Stream sentiments = builder.createStream(SentimentAnalysisConstants.Streams.SENTIMENTS, new Schema(SentimentAnalysisConstants.Field.ID, SentimentAnalysisConstants.Field.TEXT, SentimentAnalysisConstants.Field.TIMESTAMP, SentimentAnalysisConstants.Field.SENTIMENT, SentimentAnalysisConstants.Field.SCORE));
        
        builder.setSource(SentimentAnalysisConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(SentimentAnalysisConstants.Component.SOURCE, tweets);
        builder.setTupleRate(SentimentAnalysisConstants.Component.SOURCE, sourceRate);
        
        
        builder.setOperator(SentimentAnalysisConstants.Component.CLASSIFIER, new CalculateSentimentOperator(), classifierThreads);
        builder.shuffle(SentimentAnalysisConstants.Component.CLASSIFIER, tweets);
        builder.publish(SentimentAnalysisConstants.Component.CLASSIFIER, sentiments);
        
        builder.setOperator(SentimentAnalysisConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(SentimentAnalysisConstants.Component.SINK, sentiments);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return SentimentAnalysisConstants.PREFIX;
    }
    
}
