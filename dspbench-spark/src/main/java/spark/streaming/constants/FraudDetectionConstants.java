package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface FraudDetectionConstants extends BaseConstants {
    String PREFIX = "fd";
    String DEFAULT_MODEL = "frauddetection/model.txt";
    
    interface Config extends BaseConfig {
        String PREDICTOR_THREADS  = "fd.predictor.threads";
        String PARSER_THREADS     = "fd.parser.threads";
        String PREDICTOR_MODEL    = "fd.predictor.model";
        String MARKOV_MODEL_KEY   = "fd.markov.model.key";
        String LOCAL_PREDICTOR    = "fd.local.predictor";
        String STATE_SEQ_WIN_SIZE = "fd.state.seq.window.size";
        String STATE_ORDINAL      = "fd.state.ordinal";
        String DETECTION_ALGO     = "fd.detection.algorithm";
        String METRIC_THRESHOLD   = "fd.metric.threshold";
    }
}
