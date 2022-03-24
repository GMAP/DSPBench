package org.dspbench.applications.smartgrid;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.smartgrid.window.SlidingWindow;
import org.dspbench.applications.smartgrid.window.SlidingWindowCallback;
import org.dspbench.applications.smartgrid.window.SlidingWindowEntry;
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
        int type = tuple.getInt(SmartGridConstants.Field.PROPERTY);

        // we are interested only in load
        if (type == SmartGridConstants.Measurement.WORK) {
            return;
        }

        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                tuple.getLong(SmartGridConstants.Field.TIMESTAMP), tuple.getDouble(SmartGridConstants.Field.VALUE),
                tuple.getString(SmartGridConstants.Field.HOUSE_ID), tuple.getString(SmartGridConstants.Field.HOUSEHOLD_ID),
                tuple.getString(SmartGridConstants.Field.PLUG_ID));

        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    emit(new Values(entry.ts, entry.houseId, entry.houseHoldId,
                            entry.plugId, entry.value, SmartGridConstants.SlidingWindowAction.REMOVE));
                }
            }
        });

        emit(new Values(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId,
                windowEntry.plugId, windowEntry.value, SmartGridConstants.SlidingWindowAction.ADD));
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
