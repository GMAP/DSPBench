package flink.application.sentimentanalysis;

import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.constants.SentimentAnalysisConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.Locale;

public class SentimentCalculator implements FlatMapFunction<Tuple4<String, String, Date, String>, Tuple6<String, String, Date, String, Double, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentCalculator.class);

    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
            .withLocale(Locale.ENGLISH);

    private SentimentClassifier classifier;

    public void initialize(Configuration config, StreamExecutionEnvironment env, String prefix) {
        String classifierType = config.getString(SentimentAnalysisConstants.Conf.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public void flatMap(Tuple4<String, String, Date, String> input, Collector<Tuple6<String, String, Date, String, Double, String>> out) {
        String time = input.getField(3);
        String tweetId = input.getField(0);
        String text = input.getField(1);
        Date timestamp = input.getField(2);

        SentimentResult result = classifier.classify(text);

        out.collect(new Tuple6<String, String, Date, String, Double, String>(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore(), time));
        //super.calculateThroughput();
    }
}
