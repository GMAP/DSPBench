package flink.application.sentimentanalysis;

import flink.application.AbstractApplication;
import flink.constants.MachineOutlierConstants;
import flink.constants.SentimentAnalysisConstants;
import flink.parsers.JsonTweetParser;
import flink.source.InfSourceFunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SentimentAnalysis extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysis.class);
    private int sourceThreads;
    private int parserThreads;
    private int classifierThreads;
    private long runTimeSec;

    public SentimentAnalysis(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        sourceThreads = config.getInteger(SentimentAnalysisConstants.Conf.SOURCE_THREADS, 1);
        parserThreads = config.getInteger(SentimentAnalysisConstants.Conf.PARSER_THREADS, 1);
        classifierThreads = config.getInteger(SentimentAnalysisConstants.Conf.CLASSIFIER_THREADS, 1);

        runTimeSec = config.getInteger(String.format(SentimentAnalysisConstants.Conf.RUNTIME, getConfigPrefix()), 60);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        // DataStream<String> data = createSource();

        InfSourceFunction source = new InfSourceFunction(config, getConfigPrefix(), runTimeSec);
        DataStream<String> data = env.addSource(source).setParallelism(sourceThreads);

        // Parser
        DataStream<Tuple3<String, String, Date>> dataParse = data.map(new JsonTweetParser(config))
                .setParallelism(parserThreads);

        // Process
        DataStream<Tuple5<String, String, Date, String, Double>> calculate = dataParse
                .filter(value -> (value != null)).flatMap(new SentimentCalculator(config))
                .setParallelism(classifierThreads);

        // Sink
        createSinkSA(calculate);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return SentimentAnalysisConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
