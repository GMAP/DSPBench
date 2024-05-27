package flink.application.smartgrid;

import flink.application.smartgrid.window.SlidingWindow;
import flink.application.smartgrid.window.SlidingWindowCallback;
import flink.application.smartgrid.window.SlidingWindowEntry;
import flink.constants.SmartGridConstants;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SlideWindow extends Metrics implements FlatMapFunction<Tuple7<String, Long, Double, Integer, String, String, String>, Tuple6<Long, String, String, String, Double, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(SlideWindow.class);

    private static SlidingWindow window;

    Configuration config;

    public SlideWindow(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    private SlidingWindow createWindow(){
        if (window == null) {
            window =new SlidingWindow(60 * 60);
        }

        return window;
    }

    @Override
    public void flatMap(Tuple7<String, Long, Double, Integer, String, String, String> input, Collector<Tuple6<Long, String, String, String, Double, Integer>> out) {
        super.initialize(config);
        createWindow();
        int type = input.getField(3);
        super.incReceived();

        // we are interested only in load
        if (type == SmartGridConstants.Measurement.WORK) {
            return;
        }

        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                input.getField(1), input.getField(2),
                input.getField(6), input.getField(5),
                input.getField(4));

        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    super.incEmitted();
                    out.collect(new Tuple6<Long, String, String, String, Double, Integer>(entry.ts, entry.houseId, entry.houseHoldId, entry.plugId, entry.value, -1));
                }
            }
        });
        super.incEmitted();
        out.collect(new Tuple6<Long, String, String, String, Double, Integer>(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId, windowEntry.plugId, windowEntry.value, 1));
    }

    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private final String houseId;
        private final String houseHoldId;
        private final String plugId;
        private final long ts;
        private final double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId, String houseHoldId, String plugId) {
            this.ts = ts;
            this.value = value;
            this.houseId = houseId;
            this.houseHoldId = houseHoldId;
            this.plugId = plugId;
        }

        @Override
        public long getTime() {
            return ts;
        }
    }
}
