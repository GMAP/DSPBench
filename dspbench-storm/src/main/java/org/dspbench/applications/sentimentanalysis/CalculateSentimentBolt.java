package org.dspbench.applications.sentimentanalysis;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.*;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.applications.sentimentanalysis.sentiment.SentimentResult;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.applications.sentimentanalysis.SentimentAnalysisConstants.Conf;
import org.dspbench.applications.sentimentanalysis.SentimentAnalysisConstants.Field;
import org.dspbench.applications.sentimentanalysis.sentiment.SentimentClassifier;
import org.dspbench.applications.sentimentanalysis.sentiment.SentimentClassifierFactory;

/**
 * Breaks each tweet into words and calculates the sentiment of each tweet and associates the sentiment value to the State
 * and logs the same to the console and also logs to the file.
 * https://github.com/voltas/real-time-sentiment-analytic
 * 
 * @author Saurabh Dubey <147am@gmail.com>
 */
public class CalculateSentimentBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CalculateSentimentBolt.class);

    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
                                                                             .withLocale(Locale.ENGLISH);
    
    private SentimentClassifier classifier;

    @Override
    public void initialize() {
        String classifierType = config.getString(Conf.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ID, Field.TEXT, Field.TIMESTAMP, Field.SENTIMENT, Field.SCORE, Field.INITTIME);
    }

    @Override
    public void execute(Tuple input) {
        String tweetId = (String) input.getValueByField(Field.ID);
        String text = (String) input.getValueByField(Field.TWEET);
        Date timestamp = (Date) input.getValueByField(Field.TIMESTAMP);
        String time = (String) input.getValueByField(Field.INITTIME);

        SentimentResult result = classifier.classify(text);

        collector.emit(input, new Values(tweetId, text, timestamp, result.getSentiment().toString(), result.getScore(), time));
        collector.ack(input);
        super.calculateThroughput();
    }
}