package flink.application.sentimentanalysis;

import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.constants.SentimentAnalysisConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SentimentCalculator extends RichFlatMapFunction<Tuple3<String, String, Date>, Tuple5<String, String, Date, String, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentCalculator.class);

    private final SentimentClassifier classifier;

    Configuration config;
    Metrics metrics = new Metrics();

    public SentimentCalculator(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
        String classifierType = config.getString(SentimentAnalysisConstants.Conf.CLASSIFIER_TYPE,
                SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public void flatMap(Tuple3<String, String, Date> input,
            Collector<Tuple5<String, String, Date, String, Double>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        String tweetId = input.getField(0);
        String text = input.getField(1);
        Date timestamp = input.getField(2);

        SentimentResult result = classifier.classify(text);

        out.collect(new Tuple5<String, String, Date, String, Double>(tweetId, text, timestamp,
                result.getSentiment().toString(), result.getScore()));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
