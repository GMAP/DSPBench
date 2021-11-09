package com.streamer.examples.sentimentanalysis;

import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.base.task.AbstractTask;
import com.streamer.base.task.BasicTask;
import static com.streamer.examples.sentimentanalysis.SentimentAnalysisConstants.*;
import com.streamer.utils.Configuration;
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
        
        classifierThreads = config.getInt(Config.CLASSIFIER_THREADS, 1);
    }
    
    public void initialize() {
        Stream tweets = builder.createStream(Streams.TWEETS, new Schema(Field.ID, Field.TEXT, Field.TIMESTAMP));
        Stream sentiments = builder.createStream(Streams.SENTIMENTS, new Schema(Field.ID, Field.TEXT, Field.TIMESTAMP, Field.SENTIMENT, Field.SCORE));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, tweets);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        
        builder.setOperator(Component.CLASSIFIER, new CalculateSentimentOperator(), classifierThreads);
        builder.shuffle(Component.CLASSIFIER, tweets);
        builder.publish(Component.CLASSIFIER, sentiments);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, sentiments);
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
