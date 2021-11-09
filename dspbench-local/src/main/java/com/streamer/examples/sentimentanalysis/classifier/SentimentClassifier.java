package com.streamer.examples.sentimentanalysis.classifier;

import com.streamer.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
