package com.streamer.examples.sentimentanalysis;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.sentimentanalysis.SentimentAnalysisConstants.*;
import com.streamer.examples.sentimentanalysis.classifier.SentimentClassifier;
import com.streamer.examples.sentimentanalysis.classifier.SentimentClassifierFactory;
import com.streamer.examples.sentimentanalysis.classifier.SentimentResult;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateSentimentOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSentimentOperator.class);

    private SentimentClassifier classifier;

    @Override
    public void initialize() {
        String classifierType = config.getString(Config.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }
    
    public void process(Tuple input) {
        String tweetId = input.getString(Field.ID);
        String text = input.getString(Field.TEXT);
        Date timestamp = (Date) input.getValue(Field.TIMESTAMP);
        
        SentimentResult result = classifier.classify(text);
        
        emit(input, new Values(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore()));
    }
}