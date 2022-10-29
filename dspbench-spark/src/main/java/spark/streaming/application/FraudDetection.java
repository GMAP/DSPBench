package spark.streaming.application;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.FraudDetectionConstants;
import spark.streaming.constants.SentimentAnalysisConstants;
import spark.streaming.function.*;
import spark.streaming.util.Configuration;

public class FraudDetection extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);
    private int parserThreads;
    private int predictorThreads;

    public FraudDetection(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(FraudDetectionConstants.Config.PARSER_THREADS, 1);
        predictorThreads = config.getInt(FraudDetectionConstants.Config.PREDICTOR_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {
        var rawRecords = createSource();

        var records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSTransationParser(config), Encoders.kryo(Row.class));

       var predictors = records
                .repartition(predictorThreads)
                .map(new SSFraudPredictor(config), Encoders.kryo(Row.class))
                .filter(new SSFilterNull<>());

        return createSink(predictors);
    }

    @Override
    public String getConfigPrefix() {
        return FraudDetectionConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
