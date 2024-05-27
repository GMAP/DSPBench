package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface SmartGridConstants extends BaseConstants {
    String PREFIX = "sg";
    
    interface Config extends BaseConfig {
        String SLICE_LENGTH = "sg.slice.length";

        String SLIDING_WINDOW_THREADS   = "sg.sliding_window.threads";
        String GLOBAL_MEDIAN_THREADS    = "sg.global_median.threads";
        String PLUG_MEDIAN_THREADS      = "sg.plug_median.threads";
        String HOUSE_LOAD_THREADS       = "sg.house_load.threads";
        String PLUG_LOAD_THREADS        = "sg.plug_load.threads";
        String OUTLIER_DETECTOR_THREADS = "sg.outlier_detector.threads";

        String HOUSE_LOAD_FREQUENCY     = "sg.house_load.frequency";
        String PLUG_LOAD_FREQUENCY      = "sg.plug_load.frequency";

        // generator configs
        String GENERATOR_INTERVAL_SECONDS = "sg.generator.interval_seconds";
        String GENERATOR_NUM_HOUSES       = "sg.generator.houses.num";
        String GENERATOR_HOUSEHOLDS_MIN   = "sg.generator.households.min";
        String GENERATOR_HOUSEHOLDS_MAX   = "sg.generator.households.max";
        String GENERATOR_PLUGS_MIN        = "sg.generator.plugs.min";
        String GENERATOR_PLUGS_MAX        = "sg.generator.plugs.max";
        String GENERATOR_LOADS            = "sg.generator.load.list";
        String GENERATOR_LOAD_OSCILLATION = "sg.generator.load.oscillation";
        String GENERATOR_PROBABILITY_ON   = "sg.generator.on.probability";
        String GENERATOR_ON_LENGTHS       = "sg.generator.on.lengths";
    }

    interface Component {
        String OUTLIER_SINK     = "sg.outlier.sink.class";
        String PREDICTION_SINK  = "sg.prediction.sink.class";
    }

    interface Measurement {
        int WORK = 0;
        int LOAD = 1;
    }

    interface SlidingWindowAction {
        int ADD = 1;
        int REMOVE = -1;
    }
}
