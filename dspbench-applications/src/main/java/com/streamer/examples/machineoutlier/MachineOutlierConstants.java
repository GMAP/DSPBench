package com.streamer.examples.machineoutlier;

import com.streamer.base.constants.BaseConstants;

public interface MachineOutlierConstants extends BaseConstants {
    String PREFIX = "mo";
    
    interface Config extends BaseConfig {
        String GENERATOR_NUM_MACHINES = "mo.generator.num_machines";
        
        String SCORER_THREADS               = "mo.scorer.threads";
        String SCORER_DATA_TYPE             = "mo.scorer.data_type";
        String ANOMALY_SCORER_THREADS       = "mo.anomaly_scorer.threads";
        String ANOMALY_SCORER_WINDOW_LENGTH = "mo.anomaly_scorer.window_length";
        String ANOMALY_SCORER_LAMBDA        = "mo.anomaly_scorer.lambda";
        String ALERT_TRIGGER_THREADS        = "mo.alert_trigger.threads";
        String ALERT_TRIGGER_TOPK           = "mo.alert_trigger.topk";
    }
    
    interface Component extends BaseComponent {
        String SCORER = "scorerOperator";
        String ANOMALY_SCORER = "anomalyScorerOperator";
        String ALERT_TRIGGER = "alertTriggerOperator";
    }
    
    interface Field {
        String ID = "id";
        String TIMESTAMP = "timestamp";
        String IP = "ip";
        //String ENTITY_ID = "entityID";
        //String METADATA = "metadata";
        String OBSERVATION = "observation";
        String IS_ABNORMAL = "isAbnormal";
        
        String SCORE = "score";
        String ANOMALY_SCORE = "anomalyScore";
        
        /*String DATAINST_ANOMALY_SCORE = "dataInstanceAnomalyScore";
        String DATAINST_SCORE = "dataInstanceScore";
        String CUR_DATAINST_SCORE = "curDataInstanceScore";
        String STREAM_ANOMALY_SCORE = "streamAnomalyScore";
        
        String ANOMALY_STREAM = "anomalyStream";
        */
    }
    
    interface Streams {
        String READINGS  = "readingStream";
        String SCORES    = "scoreStream";
        String ANOMALIES = "anomalyScoreStream";
        String ALERTS    = "alertStream";
    }
}
