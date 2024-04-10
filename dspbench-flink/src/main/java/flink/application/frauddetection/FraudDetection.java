package flink.application.frauddetection;

import flink.application.AbstractApplication;
import flink.constants.FraudDetectionConstants;
import flink.parsers.TransactionParser;
import flink.source.InfSourceFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudDetection extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(FraudDetection.class);

    private int sourceThreads;
    private int parserThreads;
    private int predictorThreads;
    private long runTimeSec;

    public FraudDetection(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        sourceThreads = config.getInteger(FraudDetectionConstants.Conf.SOURCE_THREADS, 1);
        parserThreads = config.getInteger(FraudDetectionConstants.Conf.PARSER_THREADS, 1);
        predictorThreads = config.getInteger(FraudDetectionConstants.Conf.PREDICTOR_THREADS, 1);

        runTimeSec = config.getInteger(String.format(FraudDetectionConstants.Conf.RUNTIME, getConfigPrefix()), 60);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        // DataStream<String> data = createSource();

        InfSourceFunction source = new InfSourceFunction(config, getConfigPrefix(), runTimeSec);
        DataStream<String> data = env.addSource(source).setParallelism(sourceThreads);

        // Parser
        DataStream<Tuple3<String, String, String>> dataParse = data.map(new TransactionParser(config))
                .setParallelism(parserThreads);

        // Process
        DataStream<Tuple4<String, Double, String, String>> fraudPredict = dataParse.keyBy(value -> value.f0)
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
