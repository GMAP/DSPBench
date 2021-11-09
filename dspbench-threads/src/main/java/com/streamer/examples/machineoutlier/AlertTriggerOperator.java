package com.streamer.examples.machineoutlier;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Field;
import com.streamer.examples.utils.BFPRT;
import java.util.ArrayList;
import java.util.List;

/**
 * The AlertTriggerBolt triggers an alert if a stream is identified as abnormal.
 * @author yexijiang
 *
 */
public class AlertTriggerOperator extends BaseOperator {
    private static final double dupper = Math.sqrt(2);
    private long previousTimestamp;
    private List<Tuple> streamList;
    private double minDataInstanceScore = Double.MAX_VALUE;
    private double maxDataInstanceScore = 0;

    @Override
    public void initialize() {
        previousTimestamp = 0;
        streamList = new ArrayList<Tuple>();
    }

    @Override
    public void process(Tuple input) {
        long timestamp = input.getLong(Field.TIMESTAMP);
        
        if (timestamp > previousTimestamp) {
            // new batch of stream scores
            if (!streamList.isEmpty()) {
                List<Tuple> abnormalStreams = identifyAbnormalStreams();
                int medianIdx = (int) streamList.size() / 2;
                double minScore = abnormalStreams.get(0).getDouble(Field.ANOMALY_SCORE);
                double medianScore = abnormalStreams.get(medianIdx).getDouble(Field.ANOMALY_SCORE);
                
                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore = streamProfile.getDouble(Field.ANOMALY_SCORE);
                    double curDataInstScore = streamProfile.getDouble(Field.SCORE);
                    boolean isAbnormal = false;

                    // current stream score deviates from the majority
                    if ((streamScore > 2 * medianScore - minScore) && (streamScore > minScore + 2 * dupper)) {
                        // check whether cur data instance score return to normal
                        if (curDataInstScore > 0.1 + minDataInstanceScore) {
                            isAbnormal = true;
                        }
                    }

                    if (isAbnormal) {
                        emit(input, new Values(streamProfile.getString(Field.ID),
                                streamScore, streamProfile.getLong(Field.TIMESTAMP),
                                isAbnormal, streamProfile.getValue(Field.OBSERVATION)));
                    }
                }
                
                streamList.clear();
                minDataInstanceScore = Double.MAX_VALUE;
                maxDataInstanceScore = 0;
            }

            previousTimestamp = timestamp;
        }

        double dataInstScore = input.getDouble(Field.SCORE);
        if (dataInstScore > maxDataInstanceScore) {
            maxDataInstanceScore = dataInstScore;
        }
        
        if (dataInstScore < minDataInstanceScore) {
            minDataInstanceScore = dataInstScore;
        }

        streamList.add(input);
    }

    /**
     * Identify the abnormal streams.
     * @return
     */
    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<Tuple>();
        int medianIdx = (int)(streamList.size() / 2);
        BFPRT.bfprt(streamList, medianIdx);
        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}