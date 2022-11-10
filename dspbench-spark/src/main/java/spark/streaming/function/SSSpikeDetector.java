package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.util.Configuration;

/**
 * @author luandopke
 */
public class SSSpikeDetector extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSpikeDetector.class);
    private double spikeThreshold;

    public SSSpikeDetector(Configuration config) {
        super(config);
        spikeThreshold = config.getDouble(SpikeDetectionConstants.Config.SPIKE_DETECTOR_THRESHOLD, 0.03d);
    }

    @Override
    public Row call(Row value) throws Exception {
        super.calculateThroughput();
        int deviceID = value.getInt(0);
        double movingAverageInstant = value.getDouble(1);
        double nextDouble = value.getDouble(2);

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            return RowFactory.create(deviceID, movingAverageInstant, nextDouble, "spike detected", value.get(value.size() - 1));
        }
        return null;
    }
}