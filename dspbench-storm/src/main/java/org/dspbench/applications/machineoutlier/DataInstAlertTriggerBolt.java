package org.dspbench.applications.machineoutlier;

import java.util.ArrayList;
import java.util.List;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import org.dspbench.applications.machineoutlier.MachineOutlierConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.math.BFPRT;

/**
 * The alert is triggered solely by the anomaly score of current data instance.
 * @author Yexi Jiang (http://users.cs.fiu.edu/~yjian004)
 *
 */
public class DataInstAlertTriggerBolt extends AbstractBolt {
    private static final double dupper = 1.0;
    private long previousTimestamp;
    private List<Tuple> streamList;

    @Override
    public void initialize() {
        previousTimestamp = 0;
        streamList = new ArrayList<>();
    }

    @Override
    public void execute(Tuple input) {
        long timestamp = input.getLongByField(MachineOutlierConstants.Field.TIMESTAMP);
        
        if (timestamp > previousTimestamp) {
            // new batch of stream scores
            if (!streamList.isEmpty()) {
                List<Tuple> abnormalStreams = identifyAbnormalStreams();
                int medianIdx = (int)Math.round(streamList.size() / 2);
                double minScore = abnormalStreams.get(0).getDouble(1);
                double medianScore = abnormalStreams.get(medianIdx).getDouble(1);
                
                for (int i = 0; i < abnormalStreams.size(); ++i) {
                    Tuple streamProfile = abnormalStreams.get(i);
                    double streamScore = streamProfile.getDouble(1);
                    boolean isAbnormal = false;
                    
                    if (streamScore > 2 * medianScore - minScore) {
                        isAbnormal = true;
                    }
                    
                    collector.emit(new Values(streamProfile.getString(0), 
                            streamProfile.getDouble(1), streamProfile.getLong(2), 
                            isAbnormal, streamProfile.getValue(3)));
                }
                
                streamList.clear();
            }

            previousTimestamp = timestamp;
        }

        streamList.add(input);
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(MachineOutlierConstants.Field.ANOMALY_STREAM, MachineOutlierConstants.Field.STREAM_ANOMALY_SCORE, MachineOutlierConstants.Field.TIMESTAMP, MachineOutlierConstants.Field.IS_ABNORMAL, MachineOutlierConstants.Field.OBSERVATION);
    }

    /**
     * Identify the abnormal streams.
     * @return
     */
    private List<Tuple> identifyAbnormalStreams() {
        List<Tuple> abnormalStreamList = new ArrayList<>();

        int medianIdx = (int)Math.round(streamList.size() / 2);
        Tuple medianTuple = BFPRT.bfprt(streamList, medianIdx);
        double minScore = Double.MAX_VALUE;
        
        for (int i = 0; i < medianIdx; ++i) {
            double score = streamList.get(i).getDouble(1);
            if (score < minScore) {
                minScore = score; 
            }
        }

        double medianScore = medianTuple.getDouble(1);

        abnormalStreamList.addAll(streamList);
        return abnormalStreamList;
    }
}