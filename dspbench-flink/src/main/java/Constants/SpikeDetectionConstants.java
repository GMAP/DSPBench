package Constants;

/**
 *  @author  Alessandra Fais
 *  @version May 2019
 *
 *  Constants peculiar of the SpikeDetection application.
 */
public interface SpikeDetectionConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/spikedetection/sd.properties";
    String DEFAULT_TOPO_NAME = "SpikeDetection";
    double DEFAULT_THRESHOLD = 0.03d;
    
    interface Conf {
        String SPOUT_PATH = "sd.spout.path";
        String PARSER_VALUE_FIELD = "sd.parser.value_field";
        String MOVING_AVERAGE_WINDOW = "sd.moving_average.window";
        String SPIKE_DETECTOR_THRESHOLD = "sd.spike_detector.threshold";

        String SPOUT_THREADS = "sd.spout.threads";
        String MOVING_AVERAGE_THREADS = "sd.moving_average.threads";
        String SPIKE_DETECTOR_THREADS = "sd.spike_detector.threads";
        String SINK_THREADS = "sd.sink.threads";
        String ALL_THREADS = "sd.all.threads"; // useful only with Flink
    }
    
    interface Component extends BaseComponent {
        String MOVING_AVERAGE = "moving_average";
        String SPIKE_DETECTOR = "spike_detector";
    }
    
    interface Field extends BaseField {
        String DEVICE_ID = "deviceID";
        String VALUE = "value";
        String MOVING_AVG = "movingAverage";
    }

    interface DatasetParsing {
        int DATE_FIELD = 0;
        int TIME_FIELD = 1;
        int EPOCH_FIELD = 2;
        int DEVICEID_FIELD = 3;
        int TEMP_FIELD = 4;
        int HUMID_FIELD = 5;
        int LIGHT_FIELD = 6;
        int VOLT_FIELD = 7;
    }
}
