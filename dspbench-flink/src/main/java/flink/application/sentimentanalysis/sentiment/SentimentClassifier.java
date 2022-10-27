package flink.application.sentimentanalysis.sentiment;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize();
    public SentimentResult classify(String str);
}
