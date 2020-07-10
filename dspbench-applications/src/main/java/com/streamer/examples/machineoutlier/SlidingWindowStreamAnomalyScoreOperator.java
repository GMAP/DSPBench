package com.streamer.examples.machineoutlier;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Config;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * Summing up all the data instance scores as the stream anomaly score.
 * @author yexijiang
 *
 */
public class SlidingWindowStreamAnomalyScoreOperator extends BaseOperator {
    private Map<String, Queue<Double>> slidingWindowMap;
    private int windowLength;
    private long previousTimestamp;

    @Override
    public void initialize() {
        windowLength      = config.getInt(Config.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        slidingWindowMap  = new HashMap<String, Queue<Double>>();
        previousTimestamp = 0;
    }

    @Override
    public void process(Tuple input) {
        String obsId = input.getString(Field.ID);
        long timestamp = input.getLong(Field.TIMESTAMP);
        double instanceScore = input.getDouble(Field.SCORE);
        
        Queue<Double> slidingWindow = slidingWindowMap.get(obsId);
        if (slidingWindow == null) {
            slidingWindow = new LinkedList<Double>();
        }

        // update sliding window
        slidingWindow.add(instanceScore);
        if (slidingWindow.size() > this.windowLength) {
            slidingWindow.poll();
        }
        slidingWindowMap.put(obsId, slidingWindow);

        double sumScore = 0.0;
        for (double score : slidingWindow) {
            sumScore += score;
        }
        
        emit(input, new Values(obsId, sumScore, timestamp, input.getValue(Field.OBSERVATION), instanceScore));
    }
}
