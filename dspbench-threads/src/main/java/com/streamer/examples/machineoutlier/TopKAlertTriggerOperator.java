package com.streamer.examples.machineoutlier;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Config;
import com.streamer.examples.machineoutlier.MachineOutlierConstants.Field;
import com.streamer.examples.utils.Sorter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Always trigger the top-K objects as abnormal.
 * @author yexijiang
 *
 */
public class TopKAlertTriggerOperator extends BaseOperator {
    private int k;
    private long previousTimestamp;
    private List<Tuple> streamList;

    @Override
    public void initialize() {
        k = config.getInt(Config.ALERT_TRIGGER_TOPK, 3);
        previousTimestamp = 0;
        streamList = new ArrayList<Tuple>();
    }

    @Override
    public void process(Tuple input) {
        long timestamp = input.getLong(Field.TIMESTAMP);
        
        // new batch
        if (timestamp > previousTimestamp) {
            // sort the tuples in stream list
            Sorter.quicksort(streamList, new Comparator<Tuple>() {
                @Override
                public int compare(Tuple o1, Tuple o2) {
                    double score1 = o1.getDouble(Field.ANOMALY_SCORE);
                    double score2 = o2.getDouble(Field.ANOMALY_SCORE);
                    if (score1 < score2) {
                        return -1;
                    } else if (score1 > score2) {
                        return 1;
                    }
                    return 0;
                }
            });

            //	treat the top-K as abnormal
            int realK = streamList.size() < k ? streamList.size() : k;
            for (int i = 0; i < streamList.size(); ++i) {
                Tuple streamProfile = streamList.get(i);
                boolean isAbnormal = false;
                
                // last three stream are marked as abnormal
                if (i >= streamList.size() - 3) {
                    isAbnormal = true;
                }
                
                emit(new Values(streamProfile.getString(Field.ID), streamProfile.getDouble(Field.ANOMALY_SCORE),
                        streamProfile.getLong(Field.TIMESTAMP), isAbnormal, streamProfile.getValue(Field.OBSERVATION)));
            }

            previousTimestamp = timestamp;
            
            // clear the cache
            streamList.clear();
        }

        streamList.add(input);
    }
}