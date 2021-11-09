package com.streamer.examples.frauddetection;

import com.streamer.base.constants.BaseConstants;
import com.streamer.base.constants.BaseConstants.BaseConfig;

public interface FraudDetectionConstants extends BaseConstants {
    String PREFIX = "fd";
    String DEFAULT_MODEL = "frauddetection/model.txt";
    
    interface Config extends BaseConfig {
        String PREDICTOR_THREADS  = "fd.predictor.threads";
        String PREDICTOR_MODEL    = "fd.predictor.model";
        String MARKOV_MODEL_KEY   = "fd.markov.model.key";
        String LOCAL_PREDICTOR    = "fd.local.predictor";
        String STATE_SEQ_WIN_SIZE = "fd.state.seq.window.size";
        String STATE_ORDINAL      = "fd.state.ordinal";
        String DETECTION_ALGO     = "fd.detection.algorithm";
        String METRIC_THRESHOLD   = "fd.metric.threshold";
    }
    
    interface Component extends BaseComponent {
        String PREDICTOR = "predictorOperator";
    }
    
    interface Streams {
        String TRANSACTIONS = "transactionStream";
        String PREDICTIONS  = "predictionStream";
    }
    
    interface Field {
        String ENTITY_ID   = "entityID";
        String RECORD_DATA = "recordData";
        String SCORE       = "score";
        String STATES      = "states";
    }
    
    interface PredictorModel {
        String MARKOV_MODEL = "mm";
    }
}
