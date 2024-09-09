package flink.application.reinforcementlearner;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import flink.application.reinforcementlearner.learner.ReinforcementLearnerFactory;
import flink.application.reinforcementlearner.learner.ReinforcementLearnerInterface;
import flink.constants.ReinforcementLearnerConstants;
import flink.util.Configurations;
import flink.util.Metrics;

public class Learner extends RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, String[]>> {
    private ReinforcementLearnerInterface learner;
    Configuration config;

    Metrics metrics = new Metrics();

    public Learner(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;

        String learnerType = config.getString(ReinforcementLearnerConstants.Conf.LEARNER_TYPE, "intervalEstimator");
        String[] actions   = config.getString(ReinforcementLearnerConstants.Conf.LEARNER_ACTIONS, "page1,page2,page3").split(",");
        
        learner =  ReinforcementLearnerFactory.create(learnerType, actions, config);
    }

    @Override
    public void flatMap1(Tuple2<String, Integer> value, Collector<Tuple2<String, String[]>> out) throws Exception {
        // EVENT
        metrics.initialize(config, this.getClass().getSimpleName());
        String eventID = value.getField(0);
        int roundNum   = value.getField(1);

        String[] actions = learner.nextActions(roundNum);
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        out.collect(new Tuple2<String,String[]>(eventID, actions));
    }

    @Override
    public void flatMap2(Tuple2<String, Integer> value, Collector<Tuple2<String, String[]>> out) throws Exception {
        // REWARD
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String action = value.getField(0);
        int reward    = value.getField(1);
        
        learner.setReward(action, reward);
    }

    
}