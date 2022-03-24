package org.dspbench.applications.machineoutlier;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.applications.machineoutlier.MachineOutlierConstants;
import org.dspbench.bolt.AbstractBolt;

/**
 * Summing up all the data instance scores as the stream anomaly score.
 * @author yexijiang
 *
 */
public class SlidingWindowStreamAnomalyScoreBolt extends AbstractBolt {
    // hold the recent scores for each stream
    private Map<String, Queue<Double>> slidingWindowMap;
    private int windowLength;
    private long previousTimestamp;

    @Override
    public void initialize() {
        windowLength = config.getInt(MachineOutlierConstants.Conf.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        slidingWindowMap = new HashMap<>();
        previousTimestamp = 0;
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(MachineOutlierConstants.Field.TIMESTAMP);
        String id = input.getStringByField(MachineOutlierConstants.Field.ID);
        double dataInstanceAnomalyScore = input.getDoubleByField(MachineOutlierConstants.Field.DATAINST_ANOMALY_SCORE);
        
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
        
        collector.emit(new Values(id, sumScore, timestamp, input.getValue(3), dataInstanceAnomalyScore));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(MachineOutlierConstants.Field.ID, MachineOutlierConstants.Field.STREAM_ANOMALY_SCORE, MachineOutlierConstants.Field.TIMESTAMP, MachineOutlierConstants.Field.OBSERVATION, MachineOutlierConstants.Field.CUR_DATAINST_SCORE);
    }
}
