package com.streamer.examples.machineoutlier;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Config;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Field;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * DataStreamAnomalyScoreBolt keeps and update the stream anomaly score for each stream.
 * @author yexijiang
 * @param <T>
 *
 */
public class DataStreamAnomalyScoreOperator<T> extends BaseOperator {
    private Map<String, StreamProfile<T>> streamProfiles;
    private double lambda;
    private double factor;
    private double threashold;
    private boolean shrinkNextRound;
    private long previousTimestamp;

    @Override
    public void initialize() {
        this.lambda            = config.getDouble(Config.ANOMALY_SCORER_LAMBDA, 0.017);
        this.factor            = Math.pow(Math.E, -lambda);
        this.threashold        = 1 / (1 - factor) * 0.5;
        this.shrinkNextRound   = false;
        this.streamProfiles    = new HashMap<String, StreamProfile<T>>();
        this.previousTimestamp = 0;
    }

    @Override
    public void process(Tuple input) {
        long timestamp = input.getLong(Field.TIMESTAMP);

        if (timestamp > previousTimestamp) {
            
            for (Map.Entry<String, StreamProfile<T>> streamProfileEntry : streamProfiles.entrySet()) {
                StreamProfile<T> streamProfile = streamProfileEntry.getValue();
                
                if (shrinkNextRound == true) {
                    streamProfile.streamAnomalyScore = 0;
                }
                
                emit(new Values(streamProfileEntry.getKey(), 
                        streamProfile.streamAnomalyScore, previousTimestamp, 
                        streamProfile.currentDataInstance, 
                        streamProfile.currentDataInstanceScore));
            }
            
            if (shrinkNextRound == true) {
                shrinkNextRound = false;
            }
            
            previousTimestamp = timestamp;
        }

        String obsId = input.getString(Field.ID);
        StreamProfile<T> profile = streamProfiles.get(obsId);
        double dataInstanceAnomalyScore = input.getDouble(Field.SCORE);
        
        if (profile == null) {
            profile = new StreamProfile<T>(obsId, (T)input.getValue(Field.OBSERVATION),
                    dataInstanceAnomalyScore, input.getDouble(Field.SCORE));
            
            streamProfiles.put(obsId, profile);
        } else {
            //	update stream score
            profile.streamAnomalyScore = profile.streamAnomalyScore * factor + dataInstanceAnomalyScore;
            profile.currentDataInstance = (T)input.getValue(Field.OBSERVATION);
            profile.currentDataInstanceScore = dataInstanceAnomalyScore;
            
            if (profile.streamAnomalyScore > threashold) {
                shrinkNextRound = true;
            }
            
            streamProfiles.put(obsId, profile);
        }
    }

    /**
     * Keeps the profile of the stream.
     * @author yexijiang
     *
     * @param <T>
     */
    class StreamProfile<T> implements Serializable {
        String id;
        double streamAnomalyScore;
        T currentDataInstance;
        double currentDataInstanceScore;

        public StreamProfile(String id, T dataInstanceScore, double initialAnomalyScore, double currentDataInstanceScore) {
            this.id = id;
            this.streamAnomalyScore = initialAnomalyScore;
            this.currentDataInstance = dataInstanceScore;
            this.currentDataInstanceScore = currentDataInstanceScore;
        }
    }
}
