package flink.application.smartgrid;

import flink.constants.SmartGridConstants;
import flink.util.Metrics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class HouseLoadPredict extends Metrics implements WindowFunction<Tuple8<String, Long, Double, Integer, String, String, String, String>, Tuple4<Long,String, Double, String>, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(HouseLoadPredict.class);
    protected static long sliceLength = 60L;
    protected static long currentSliceStart;

    protected String inittime = "";
    protected int tickCounter = 0;
    protected static Map<String, AverageTracker> trackers;
    protected static Map<String, SummaryArchive> archiveMap;

    Configuration config;

    public HouseLoadPredict(Configuration config) {
        super.initialize(config);
        this.config = config;
        sliceLength = 60L;
    }

    private Map<String, AverageTracker>  track() {
        if (trackers == null) {
            trackers = new HashMap<String, AverageTracker>();
        }

        return trackers;
    }

    private Map<String, SummaryArchive>  archMap() {
        if (archiveMap == null) {
            archiveMap = new HashMap<String, SummaryArchive>();
        }

        return archiveMap;
    }

    @Override
    public void apply(String s, TimeWindow window, Iterable<Tuple8<String, Long, Double, Integer, String, String, String, String>> input, Collector<Tuple4<Long, String, Double, String>> out) throws Exception {
        super.initialize(config);
        for (Tuple8<String, Long, Double, Integer, String, String, String, String> in : input) {

            if (inittime.equals("")) {
                inittime = in.getField(7);
            }

            int type = in.getField(3);

            if (type == SmartGridConstants.Measurement.WORK) {
                continue;
            }

            AverageTracker averageTracker = getTracker(getKey(in));
            long timestamp = in.getField(1);
            double value   = in.getField(2);

            // Initialize the very first slice
            if (currentSliceStart == 0L) {
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

        tickCounter = (tickCounter + 1) % 2;
        // time to emit
        if (tickCounter == 0) {
            for (Iterator<Tuple4<Long, String, Double, String>> it = emitOutputStream(inittime); it.hasNext(); ) {
                Tuple4<Long, String, Double, String> in = it.next();
                out.collect(new Tuple4<Long,String, Double, String>(in.f0, in.f1, in.f2, in.f3));
                super.calculateThroughput();
            }
            inittime = "";
        }
    }

    protected double predict(double currentAvg, double median) {
        return currentAvg + median;
    }

    protected Iterator<Tuple4<Long,String, Double, String>> emitOutputStream(String inittime) {

        track();
        archMap();

        List<Tuple4<Long,String, Double, String>> tuples = new ArrayList<>();

        for (String key : trackers.keySet()) {
            double currentAvg = trackers.get(key).retrieve();
            double median = 0;

            if (archiveMap.containsKey(key)) {
                median = archiveMap.get(key).getMedian();
            }

            double prediction = predict(currentAvg, median);
            long predictedTimeStamp = currentSliceStart + 2 * sliceLength;

            tuples.add(new Tuple4<Long,String, Double, String>(predictedTimeStamp, key, prediction, inittime));
        }

        return tuples.iterator();
    }
    private AverageTracker getTracker(String trackerId) {

        track();

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

        archMap();

        SummaryArchive archive;
        if (archiveMap.containsKey(trackerId)) {
            archive = archiveMap.get(trackerId);
        } else {
            archive = new SummaryArchive(sliceLength);
            archiveMap.put(trackerId, archive);
        }
        return archive;
    }
    private void startSlice() {
        for (String trackerId : trackers.keySet()) {
            AverageTracker tracker = getTracker(trackerId);
            getSummaryArchive(trackerId).archive(tracker.retrieve());
            tracker.reset();
        }
    }
    protected String getKey(Tuple tuple) {
        return tuple.getField(6);
    }
}
