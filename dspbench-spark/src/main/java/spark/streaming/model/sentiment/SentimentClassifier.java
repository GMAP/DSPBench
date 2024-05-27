package spark.streaming.model.sentiment;

import spark.streaming.util.Configuration;

import java.io.Serializable;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier extends Serializable {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
