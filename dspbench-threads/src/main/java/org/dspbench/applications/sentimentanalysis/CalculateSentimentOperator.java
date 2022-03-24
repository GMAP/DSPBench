package org.dspbench.applications.sentimentanalysis;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.sentimentanalysis.classifier.SentimentClassifier;
import org.dspbench.applications.sentimentanalysis.classifier.SentimentClassifierFactory;
import org.dspbench.applications.sentimentanalysis.classifier.SentimentResult;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalculateSentimentOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSentimentOperator.class);

    private SentimentClassifier classifier;

    @Override
    public void initialize() {
        String classifierType = config.getString(SentimentAnalysisConstants.Config.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }
    
    public void process(Tuple input) {
        String tweetId = input.getString(SentimentAnalysisConstants.Field.ID);
        String text = input.getString(SentimentAnalysisConstants.Field.TEXT);
        Date timestamp = (Date) input.getValue(SentimentAnalysisConstants.Field.TIMESTAMP);
        
        SentimentResult result = classifier.classify(text);
        
        emit(input, new Values(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore()));
    }
}