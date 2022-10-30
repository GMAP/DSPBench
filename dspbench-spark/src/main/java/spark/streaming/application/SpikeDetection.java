package spark.streaming.application;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.FraudDetectionConstants;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.function.SSFilterNull;
import spark.streaming.function.SSFraudPredictor;
import spark.streaming.function.SSTransationParser;
import spark.streaming.util.Configuration;

public class SpikeDetection extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);
    private int parserThreads;
    private int predictorThreads;

    public SpikeDetection(String appName, Configuration config) {
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

       /* var records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSTransationParser(config), Encoders.kryo(Row.class));

       var predictors = records
                .repartition(predictorThreads)
                .map(new SSFraudPredictor(config), Encoders.kryo(Row.class))
                .filter(new SSFilterNull<>());*/

        return createSink(rawRecords);
    }

    @Override
    public String getConfigPrefix() {
        return SpikeDetectionConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
