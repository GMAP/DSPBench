package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface SpikeDetectionConstants extends BaseConstants {
    String PREFIX = "sd";
    
    interface Config extends BaseConfig {
        String PARSER_VALUE_FIELD       = "sd.parser.value_field";
        String GENERATOR_COUNT          = "sd.generator.count";
        String MOVING_AVERAGE_THREADS   = "sd.moving_average.threads";
        String MOVING_AVERAGE_WINDOW    = "sd.moving_average.window";
        String SPIKE_DETECTOR_THREADS   = "sd.spike_detector.threads";
        String SPIKE_DETECTOR_THRESHOLD = "sd.spike_detector.threshold";
    }
}
