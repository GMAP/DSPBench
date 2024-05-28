package flink.constants;

public interface ReinforcementLearnerConstants extends BaseConstants {
    String PREFIX = "rl";

    interface Conf extends BaseConf {
        String EVENT_SOURCE_THREADS = "rl.event.source.threads";
        String EVENT_PARSER_THREADS = "rl.event.parser.threads";
        String REWARD_SOURCE_THREADS = "rl.reward.source.threads";
        String REWARD_PARSER_THREADS = "rl.reward.parser.threads";

        String LEARNER_THREADS = "rl.learner.threads";
        String LEARNER_TYPE    = "rl.learner.type";
        String LEARNER_ACTIONS = "rl.learner.actions";

        String RANDOM_SELECTION_PROB = "rl.random.selection.prob";
        String PROB_RED_ALGORITHM    = "rl.prob.reduction.algorithm";
        String PROB_RED_CONSTANT     = "rl.prob.reduction.constant";
        
        String BIN_WIDTH                      = "rl.bin.width";
        String CONFIDENCE_LIMIT               = "rl.confidence.limit";
        String MIN_CONFIDENCE_LIMIT           = "rl.min.confidence.limit";
        String CONFIDENCE_LIMIT_RED_STEP      = "rl.confidence.limit.reduction.step";
        String CONFIDENCE_LIMIT_RED_ROUND_INT = "rl.confidence.limit.reduction.round.interval";
        String MIN_DIST_SAMPLE                = "rl.min.reward.distr.sample";
        
        String MIN_SAMPLE_SIZE = "rl.min.sample.size";
        String MAX_REWARD      = "rl.max.reward";

        String SINK_THREADS = "rl.sink.threads";

        String DEBUG_ON = "debug.on";
        String GENERATOR_MAX_ROUNDS           = "rl.generator.max_rounds";
    }
}
