package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.FraudDetectionConstants;
import spark.streaming.function.*;
import spark.streaming.model.FraudRecord;
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
               // .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSTransationParser(config), Encoders.kryo(Row.class));

        var predictors = records
                .repartition(predictorThreads)
                .groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
                .flatMapGroupsWithState(new SSFraudPred(config), OutputMode.Update(), Encoders.kryo(FraudRecord.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

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
