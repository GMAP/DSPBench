package flink.constants;

public interface VoIPStreamConstants extends BaseConstants {
    String PREFIX = "vs";

    interface Conf extends BaseConf {
        String SOURCE_THREADS = "vs.source.threads";
        String PARSER_THREADS = "vs.parser.threads";

        String GENERATOR_POPULATION = "vs.generator.population";
        String GENERATOR_ERROR_PROB = "vs.generator.error_prob";
        
        String FILTER_NUM_ELEMENTS = "vs.%s.num_elements";
        String FILTER_BUCKETS_PEL = "vs.%s.buckets_per_element";
        String FILTER_BUCKETS_PWR = "vs.%s.buckets_per_word";
        String FILTER_BETA = "vs.%s.beta";
        
        String SCORE_THRESHOLD_MIN = "vs.%s.threshold.min";
        String SCORE_THRESHOLD_MAX = "vs.%s.threshold.max";
        
        String ACD_DECAY_FACTOR = "vs.acd.decay_factor";
        
        String FOFIR_WEIGHT = "vs.fofir.weight";
        String URL_WEIGHT = "vs.url.weight";
        String ACD_WEIGHT = "vs.acd.weight";
        
        String VAR_DETECT_APROX_SIZE = "vs.variation.aprox_size";
        String VAR_DETECT_ERROR_RATE = "vs.variation.error_rate";
        
        String VAR_DETECT_THREADS = "vs.vardetect.threads";
        String ECR_THREADS = "vs.ecr.threads";
        String RCR_THREADS = "vs.rcr.threads";
        String ENCR_THREADS = "vs.encr.threads";
        String ECR24_THREADS = "vs.ecr24.threads";
        String CT24_THREADS = "vs.ct24.threads";
        String FOFIR_THREADS = "vs.fofir.threads";
        String URL_THREADS = "vs.url.threads";
        String ACD_THREADS = "vs.acd.threads";
        String SCORER_THREADS = "vs.scorer.threads";

        String SINK_THREADS = "vs.sink.threads";
    }
}
