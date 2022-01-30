package org.dspbench.applications.sentimentanalysis.classifier;

import org.dspbench.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
