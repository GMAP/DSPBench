package Constants;

/**
 *  @author  Alessandra Fais
 *  @version May 2019
 *
 *  Constants peculiar of the FraudDetection application.
 */
public interface FraudDetectionConstants extends BaseConstants {
    String DEFAULT_MODEL = "frauddetection/model.txt";
    String DEFAULT_PROPERTIES = "/frauddetection/fd.properties";
    String DEFAULT_TOPO_NAME = "FraudDetection";

    interface Conf {
        String PREDICTOR_MODEL    = "fd.predictor.model";
        String MARKOV_MODEL_KEY   = "fd.markov.model.key";
        String LOCAL_PREDICTOR    = "fd.local.predictor";
        String STATE_SEQ_WIN_SIZE = "fd.state.seq.window.size";
        String STATE_ORDINAL      = "fd.state.ordinal";
        String DETECTION_ALGO     = "fd.detection.algorithm";
        String METRIC_THRESHOLD   = "fd.metric.threshold";

        String SPOUT_PATH = "fd.spout.path";
        String SPOUT_THREADS = "fd.spout.threads";
        String SINK_THREADS = "fd.sink.threads";
        String PREDICTOR_THREADS  = "fd.predictor.threads";
        String ALL_THREADS = "fd.all.threads"; // useful only with Flink
    }
    
    interface Component extends BaseComponent {
        String PREDICTOR = "fraud_predictor";
    }
    
    interface Field extends BaseField {
        String ENTITY_ID = "entityID";
        String RECORD_DATA = "recordData";
        String SCORE = "score";
        String STATES = "states";
    }
}
