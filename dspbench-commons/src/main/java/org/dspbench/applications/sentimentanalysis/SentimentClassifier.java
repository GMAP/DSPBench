package org.dspbench.applications.sentimentanalysis;

import org.dspbench.util.config.Configuration;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
