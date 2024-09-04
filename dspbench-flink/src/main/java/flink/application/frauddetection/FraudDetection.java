package flink.application.frauddetection;

import flink.application.AbstractApplication;
import flink.constants.FraudDetectionConstants;
import flink.parsers.TransactionParser;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetection extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    private int parserThreads;
    private int predictorThreads;

    public FraudDetection(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInteger(FraudDetectionConstants.Conf.PARSER_THREADS, 1);
        predictorThreads = config.getInteger(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple2<String, String>> dataParse = data.flatMap(new TransactionParser(config))
                .setParallelism(parserThreads);

        // Process
        DataStream<Tuple3<String, Double, String>> fraudPredict = dataParse.keyBy(value -> value.f0)
                .filter(value -> (value != null)).flatMap(new FraudPredictor(config)).setParallelism(predictorThreads);

        // Sink
        createSinkFD(fraudPredict);

        return env;
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
