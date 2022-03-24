package org.dspbench.applications.machineoutlier;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

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
        windowLength      = config.getInt(MachineOutlierConstants.Config.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        slidingWindowMap  = new HashMap<String, Queue<Double>>();
        previousTimestamp = 0;
    }

    @Override
    public void process(Tuple input) {
        String obsId = input.getString(MachineOutlierConstants.Field.ID);
        long timestamp = input.getLong(MachineOutlierConstants.Field.TIMESTAMP);
        double instanceScore = input.getDouble(MachineOutlierConstants.Field.SCORE);
        
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
        
        emit(input, new Values(obsId, sumScore, timestamp, input.getValue(MachineOutlierConstants.Field.OBSERVATION), instanceScore));
    }
}
