package com.streamer.examples.sentimentanalysis.classifier;

import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;
import com.streamer.examples.sentimentanalysis.SentimentAnalysisConstants.Config;
import com.streamer.utils.Configuration;
import java.io.File;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LingPipeClassifier implements SentimentClassifier {
    private static final Logger LOG = LoggerFactory.getLogger(LingPipeClassifier.class);
     private static final String DEFAULT_PATH = "sentimentanalysis/classifier.bin";
    private LMClassifier classifier;
    
    public void initialize(Configuration config) {
        try {
            String clsPath = config.getString(Config.LINGPIPE_CLASSIFIER_PATH, DEFAULT_PATH);
            classifier = (LMClassifier) AbstractExternalizable.readObject(new File(clsPath));
        } catch (ClassNotFoundException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Unable to initialize the sentiment classifier");
        } catch (IOException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Unable to initialize the sentiment classifier");
        }
    }

    public SentimentResult classify(String str) {
        ConditionalClassification classification = classifier.classify(str);
        String cat = classification.bestCategory();
        
        SentimentResult result = new SentimentResult();
        result.setScore(classification.score(0));
        
        if (cat.equals("pos"))
            result.setSentiment(SentimentResult.Sentiment.Positive);
        else if (cat.equals("neg"))
            result.setSentiment(SentimentResult.Sentiment.Negative);
        else
            result.setSentiment(SentimentResult.Sentiment.Neutral);
        
        return result;
    }
    
}
