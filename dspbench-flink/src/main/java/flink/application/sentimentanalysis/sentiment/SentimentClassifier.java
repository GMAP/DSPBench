package flink.application.sentimentanalysis.sentiment;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 *
 */
public interface SentimentClassifier extends Serializable {
    void initialize(Configuration config);
    SentimentResult classify(String str);
}
