package flink.application.spamfilter;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.BooleanValue;
import org.apache.kafka.common.protocol.types.Field.Bool;
import org.geotools.data.shapefile.index.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.AbstractApplication;
import flink.constants.SpamFilterConstants;
import flink.parsers.JsonEmailParser;

public class SpamFilter extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SpamFilter.class);

    private int trainingParserThreads;
    private int analysisParserThreads;
    private int tokenizerThreads;
    private int bayesThreads;

    public SpamFilter(String appName, Configuration config) {
        super(appName, config);
    }
    
    @Override
    public void initialize() {
        trainingParserThreads = config.getInteger(SpamFilterConstants.Conf.TRAINING_PARSER_THREADS, 1);
        analysisParserThreads = config.getInteger(SpamFilterConstants.Conf.ANALYSIS_PARSER_THREADS, 1);
        tokenizerThreads = config.getInteger(SpamFilterConstants.Conf.TOKENIZER_THREADS, 1);
        bayesThreads = config.getInteger(SpamFilterConstants.Conf.BAYES_RULE_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> training = createSource("training");
        DataStream<String> analysis = createSource("analysis");

        // Parser
        DataStream<Tuple3<String, String, Boolean>> trainingParser = training.flatMap(new JsonEmailParser(config, "training")).filter(value -> value != null).setParallelism(trainingParserThreads);
        DataStream<Tuple3<String, String, Boolean>> analysisParser = analysis.flatMap(new JsonEmailParser(config, "analysis")).filter(value -> value != null).setParallelism(analysisParserThreads);

        // Process
        DataStream<Tuple3<String, Word, Integer>> tokenWordProb = trainingParser.connect(analysisParser).flatMap(new TokenWordProb(config)).setParallelism(tokenizerThreads);

        DataStream<Tuple3<String,Float,Boolean>> bayes = tokenWordProb.keyBy(value -> value.f0).flatMap(new BayesRule(config)).setParallelism(bayesThreads);
        
        // Sink
        createSinkSF(bayes);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return SpamFilterConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
