package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.SentimentAnalysisConstants;
import spark.streaming.constants.TrafficMonitoringConstants.Config;
import spark.streaming.model.gis.GPSRecord;
import spark.streaming.model.gis.RoadGridList;
import spark.streaming.model.sentiment.SentimentClassifier;
import spark.streaming.model.sentiment.SentimentClassifierFactory;
import spark.streaming.model.sentiment.SentimentResult;
import spark.streaming.util.Configuration;

import java.io.IOException;
import java.sql.Date;
import java.sql.SQLException;

/**
 *
 * @author mayconbordin
 */
public class SSCalculateSentiment extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSCalculateSentiment.class);

    private SentimentClassifier classifier;

    public SSCalculateSentiment(Configuration config) {
        super(config);

        String classifierType = config.get(SentimentAnalysisConstants.Config.CLASSIFIER_TYPE, SentimentClassifierFactory.BASIC);
        classifier = SentimentClassifierFactory.create(classifierType, config);
    }


    @Override
    public Row call(Row input) throws Exception {
        super.calculateThroughput();
        String tweetId = input.getString(0);
        String text = input.getString(1);
        String timestamp = new DateTime(input.get(2)).toString();

        SentimentResult result = classifier.classify(text);

        return RowFactory.create(tweetId,
                text,
                timestamp,
                result.getSentiment().toString(),
                result.getScore(),
                input.get(input.size() - 1));
    }
}