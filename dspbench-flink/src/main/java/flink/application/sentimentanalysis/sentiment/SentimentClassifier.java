package flink.application.sentimentanalysis.sentiment;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 *
 */
public interface SentimentClassifier extends Serializable {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
