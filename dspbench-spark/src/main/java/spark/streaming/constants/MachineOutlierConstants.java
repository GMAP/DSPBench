package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface MachineOutlierConstants extends BaseConstants {
    String PREFIX = "mo";

    interface Config extends BaseConfig {
        String GENERATOR_NUM_MACHINES       = "mo.generator.num_machines";
        String SCORER_THREADS               = "mo.scorer.threads";
        String SCORER_DATA_TYPE             = "mo.scorer.data_type";
        String ANOMALY_SCORER_THREADS       = "mo.anomaly_scorer.threads";
        String ANOMALY_SCORER_WINDOW_LENGTH = "mo.anomaly_scorer.window_length";
        String ANOMALY_SCORER_LAMBDA        = "mo.anomaly_scorer.lambda";
        String ALERT_TRIGGER_THREADS        = "mo.alert_trigger.threads";
        String ALERT_TRIGGER_TOPK           = "mo.alert_trigger.topk";
    }
}
