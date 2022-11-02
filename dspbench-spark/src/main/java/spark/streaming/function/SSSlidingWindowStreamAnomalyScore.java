package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.MachineOutlierConstants;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.util.Configuration;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * @author luandopke
 */
public class SSSlidingWindowStreamAnomalyScore extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSlidingWindowStreamAnomalyScore.class);

    private Map<String, Queue<Double>> slidingWindowMap;
    private int windowLength;
    private long previousTimestamp;

    public SSSlidingWindowStreamAnomalyScore(Configuration config) {
        super(config);
        windowLength = config.getInt(MachineOutlierConstants.Config.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        slidingWindowMap = new HashMap<>();
        previousTimestamp = 0;
    }

    @Override
    public Row call(Row input) throws Exception {
        long timestamp = input.getLong(2);
        String id = input.getString(0);
        double dataInstanceAnomalyScore = input.getDouble(1);

        Queue<Double> slidingWindow = slidingWindowMap.get(id);
        if (slidingWindow == null) {
            slidingWindow = new LinkedList<>();
        }

        // update sliding window
        slidingWindow.add(dataInstanceAnomalyScore);
        if (slidingWindow.size() > this.windowLength) {
            slidingWindow.poll();
        }
        slidingWindowMap.put(id, slidingWindow);

        double sumScore = 0.0;
        for (double score : slidingWindow) {
            sumScore += score;
        }

        return RowFactory.create(id, sumScore, timestamp, input.get(3), dataInstanceAnomalyScore);
    }
}