package org.dspbench.applications.smartgrid;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.smartgrid.SmartGridConstants.*;
import org.dspbench.applications.logprocessing.StatusCountOperator;
import org.dspbench.utils.collections.FixedMap;
import org.dspbench.utils.math.OutlierTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class OutlierDetectionOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCountOperator.class);

    private FixedMap<Long, Double> globalMedianBacklog;
    private Map<String, OutlierTracker> outliers;
    private PriorityQueue<ComparableTuple> unprocessedMessages;

    @Override
    public void initialize() {
        globalMedianBacklog = new FixedMap<>(300, 300);
        outliers = new HashMap<>();
        unprocessedMessages = new PriorityQueue<>();
    }

    public void process(Tuple tuple) {
        String component = tuple.getComponentName();

        if (component.equals(Component.GLOBAL_MEDIAN)) {
            long timestamp = tuple.getLong(Field.TIMESTAMP);
            double globalMedianLoad = tuple.getDouble(Field.GLOBAL_MEDIAN_LOAD);

            globalMedianBacklog.put(timestamp, globalMedianLoad);

            // ordered based on the timestamps
            while (!unprocessedMessages.isEmpty() &&
                    unprocessedMessages.peek().tuple.getLong(Field.TIMESTAMP).equals(timestamp)) {
                Tuple perPlugMedianTuple = unprocessedMessages.poll().tuple;
                processPerPlugMedianTuple(perPlugMedianTuple);
            }
        } else {
            processPerPlugMedianTuple(tuple);
        }
    }

    private void processPerPlugMedianTuple(Tuple tuple) {
        String key     = tuple.getString(Field.PLUG_SPECIFIC_KEY);
        String houseId = key.split(":")[0];
        long timestamp = tuple.getLong(Field.TIMESTAMP);
        double value   = tuple.getDouble(Field.PER_PLUG_MEDIAN);

        if (globalMedianBacklog.containsKey(timestamp)) {
            OutlierTracker tracker;

            if (outliers.containsKey(houseId)) {
                tracker = outliers.get(houseId);
            } else {
                tracker = new OutlierTracker();
                outliers.put(houseId, tracker);
            }

            if (!tracker.isMember(key)) {
                tracker.addMember(key);
            }

            double globalMedian = globalMedianBacklog.get(timestamp);
            if (globalMedian < value) { // outlier
                if (!tracker.isOutlier(key)) {
                    tracker.addOutlier(key);
                    emit(new Values(timestamp - 24 * 60 * 60, timestamp,
                            houseId, tracker.getCurrentPercentage()));
                }
            } else {
                if (tracker.isOutlier(key)) {
                    tracker.removeOutlier(key);
                    //emit
                    emit(new Values(timestamp - 24 * 60 * 60, timestamp,
                            houseId, tracker.getCurrentPercentage()));
                }
            }
        } else {    // global median has not arrived
            unprocessedMessages.add(new ComparableTuple(tuple));
        }
    }

    private class ComparableTuple implements Serializable, Comparable<ComparableTuple> {
        private final Tuple tuple;

        private ComparableTuple(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public int compareTo(ComparableTuple o) {
            return this.tuple.getLong(Field.TIMESTAMP).compareTo(
                    o.tuple.getLong(Field.TIMESTAMP));
        }
    }
}
