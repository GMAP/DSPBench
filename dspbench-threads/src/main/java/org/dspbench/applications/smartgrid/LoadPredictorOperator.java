package org.dspbench.applications.smartgrid;

import org.dspbench.core.Operator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.utils.math.AverageTracker;
import org.dspbench.applications.utils.math.SummaryArchive;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public abstract class LoadPredictorOperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(LoadPredictorOperator.class);

    protected long currentSliceStart;
    protected long sliceLength = 60l;
    protected int tickCounter = 0;

    protected Map<String, AverageTracker> trackers;
    protected Map<String, SummaryArchive> archiveMap;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);

        trackers = new HashMap<>();
        archiveMap = new HashMap<>();
        sliceLength = config.getLong(SmartGridConstants.Config.SLICE_LENGTH, 60L);
    }

    @Override
    public void onTime() {
        LOG.info("Received tick tuple, triggering emit of current window counts. Tick counter = {}", tickCounter);
        tickCounter = (tickCounter + 1) % 2;

        // time to emit
        if (tickCounter == 0) {
            emitOutputStream();
        }
    }

    @Override
    public void process(Tuple tuple) {
        int type = tuple.getInt(SmartGridConstants.Field.PROPERTY);

        if (type == SmartGridConstants.Measurement.WORK) {
            return;
        }

        AverageTracker averageTracker = getTracker(getKey(tuple));
        long timestamp = tuple.getLong(SmartGridConstants.Field.TIMESTAMP);
        double value   = tuple.getDouble(SmartGridConstants.Field.VALUE);

        // Initialize the very first slice
        if (currentSliceStart == 0l) {
            currentSliceStart = timestamp;
        }
        // Check the slice
        // This update is within current slice.
        if ((currentSliceStart + sliceLength) >= timestamp) {
            averageTracker.track(value);
        } else {    // start a new slice
            startSlice();
            currentSliceStart = currentSliceStart + sliceLength;
            // there may be slices without any records.
            while ((currentSliceStart + sliceLength) < timestamp) {
                startSlice();
                currentSliceStart = currentSliceStart + sliceLength;
            }
            averageTracker.track(value);
        }
    }

    private AverageTracker getTracker(String trackerId) {
        AverageTracker tracker;
        if (trackers.containsKey(trackerId)) {
            tracker = trackers.get(trackerId);
        } else {
            tracker = new AverageTracker();
            trackers.put(trackerId, tracker);
        }
        return tracker;
    }

    private SummaryArchive getSummaryArchive(String trackerId) {
        SummaryArchive archive;
        if (archiveMap.containsKey(trackerId)) {
            archive = archiveMap.get(trackerId);
        } else {
            archive = new SummaryArchive(sliceLength);
            archiveMap.put(trackerId, archive);
        }
        return archive;
    }

    protected double predict(double currentAvg, double median) {
        return currentAvg + median;
    }

    private void startSlice() {
        for (String trackerId : trackers.keySet()) {
            AverageTracker tracker = getTracker(trackerId);
            getSummaryArchive(trackerId).archive(tracker.retrieve());
            tracker.reset();
        }
    }

    protected void emitOutputStream() {
        for (String key : trackers.keySet()) {
            double currentAvg = trackers.get(key).retrieve();
            double median = 0;

            if (archiveMap.containsKey(key)) {
                median = archiveMap.get(key).getMedian();
            }

            double prediction = predict(currentAvg, median);
            long predictedTimeStamp = currentSliceStart + 2 * sliceLength;
            emit(getOutputTuple(predictedTimeStamp, key, prediction));
        }
    }

    protected abstract String getKey(Tuple tuple);
    protected abstract Values getOutputTuple(long predictedTimeStamp, String keyString, double predictedValue);
}
