package com.streamer.examples.machineoutlier;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Field;
import com.streamer.examples.utils.BFPRT;
import com.streamer.examples.voipstream.VoIPSTREAMConstants;
import com.streamer.utils.Configuration;
import java.util.ArrayList;
import java.util.List;

/**
 * The alert is triggered solely by the anomaly score of current data instance.
 * @author Yexi Jiang (http://users.cs.fiu.edu/~yjian004)
 *
 */
public class DataInstAlertTriggerOperator extends BaseOperator {
    private static final double dupper = 1.0;
    private long previousTimestamp;
    private List<Tuple> streamList;

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
                int medianIdx      = (int) Math.round(streamList.size() / 2);
                double minScore    = abnormalStreams.get(0).getDouble(Field.ANOMALY_SCORE);
                double medianScore = abnormalStreams.get(medianIdx).getDouble(Field.ANOMALY_SCORE);
                
                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore  = streamProfile.getDouble(Field.ANOMALY_SCORE);
                    boolean isAbnormal  = false;
                    
                    if (streamScore > 2 * medianScore - minScore) {
                        isAbnormal = true;
                    }
                    
                    emit(input, new Values(streamProfile.getString(Field.ID), 
                            streamProfile.getDouble(Field.ANOMALY_SCORE),
                            streamProfile.getLong(Field.TIMESTAMP), 
                            isAbnormal, streamProfile.getValue(Field.OBSERVATION)));
                }
                
                streamList.clear();
            }

            previousTimestamp = timestamp;
        }

        streamList.add(input);
    }

    /**
     * Identify the abnormal streams.
     * @return
     */
    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<Tuple>();

        int medianIdx = (int)Math.round(streamList.size() / 2);
        Tuple medianTuple = BFPRT.bfprt(streamList, medianIdx);
        double minScore = Double.MAX_VALUE;
        
        for (int i = 0; i < medianIdx; ++i) {
            double score = streamList.get(i).getDouble(Field.ANOMALY_SCORE);
            if (score < minScore) {
                minScore = score; 
            }
        }

        double medianScore = medianTuple.getDouble(Field.ANOMALY_SCORE);

        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}