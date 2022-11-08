package flink.application.sentimentanalysis;

import flink.application.AbstractApplication;
import flink.constants.SentimentAnalysisConstants;
import flink.parsers.JsonTweetParser;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class SentimentAnalysis extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysis.class);
    private int classifierThreads;
    public SentimentAnalysis(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        classifierThreads = config.getInteger(SentimentAnalysisConstants.Conf.CLASSIFIER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple4<String, String, Date, String>> dataParse = data.map(new JsonTweetParser(config));

        // Process
        DataStream<Tuple6<String, String, Date, String, Double, String>> calculate = dataParse.filter(value -> (value != null)).flatMap(new SentimentCalculator(config)).setParallelism(classifierThreads);

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
