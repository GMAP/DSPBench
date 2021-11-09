package com.streamer.examples.smartgrid;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.smartgrid.SmartGridConstants.*;
import com.streamer.examples.smartgrid.window.SlidingWindow;
import com.streamer.examples.smartgrid.window.SlidingWindowCallback;
import com.streamer.examples.smartgrid.window.SlidingWindowEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SmartGridSlidingWindowOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(SmartGridSlidingWindowOperator.class);

    private SlidingWindow window;

    @Override
    public void initialize() {
        window = new SlidingWindow(1 * 60 * 60);
    }

    @Override
    public void process(Tuple tuple) {
        int type = tuple.getInt(Field.PROPERTY);

        // we are interested only in load
        if (type == Measurement.WORK) {
            return;
        }

        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                tuple.getLong(Field.TIMESTAMP), tuple.getDouble(Field.VALUE),
                tuple.getString(Field.HOUSE_ID), tuple.getString(Field.HOUSEHOLD_ID),
                tuple.getString(Field.PLUG_ID));

        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    emit(new Values(entry.ts, entry.houseId, entry.houseHoldId,
                            entry.plugId, entry.value, SlidingWindowAction.REMOVE));
                }
            }
        });

        emit(new Values(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId,
                windowEntry.plugId, windowEntry.value, SlidingWindowAction.ADD));
    }

    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private String houseId;
        private String houseHoldId;
        private String plugId;
        private long ts;
        private double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId,
                                       String houseHoldId, String plugId) {
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
