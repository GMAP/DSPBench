package flink.application.reinforcementlearner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.AbstractApplication;
import flink.constants.ReinforcementLearnerConstants;
import flink.parsers.LearnerParser;
import flink.source.CTRGenerator;
import flink.source.RewardSource;

public class ReinforcementLearner extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ReinforcementLearner.class);

    private int eventParserThreads;
    private int rewardParserThreads;
    private int learnerThreads;

    public ReinforcementLearner(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        eventParserThreads = config.getInteger(ReinforcementLearnerConstants.Conf.EVENT_PARSER_THREADS, 1);
        rewardParserThreads = config.getInteger(ReinforcementLearnerConstants.Conf.REWARD_PARSER_THREADS, 1);
        learnerThreads = config.getInteger(ReinforcementLearnerConstants.Conf.LEARNER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> event = createSource("event"); // CTRGenerator event = new CTRGenerator(config); 
        DataStream<String> reward = createSource("reward"); // RewardSource reward = new RewardSource(config);  

        // Parser
        DataStream<Tuple2<String, Integer>> eventParser = event.flatMap(new LearnerParser(config, "event")).setParallelism(rewardParserThreads); // env.addSource(event);
        DataStream<Tuple2<String, Integer>> rewardParser = reward.flatMap(new LearnerParser(config, "reward")).setParallelism(rewardParserThreads); // env.addSource(reward); 

        // Process
        DataStream<Tuple2<String, String[]>> reinforcementLearner = eventParser.rebalance().connect(rewardParser.broadcast()).flatMap(new Learner(config)).setParallelism(learnerThreads);

        // Sink
        createSinkRL(reinforcementLearner);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return ReinforcementLearnerConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
