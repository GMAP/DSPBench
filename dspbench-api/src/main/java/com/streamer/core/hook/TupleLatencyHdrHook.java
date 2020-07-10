package com.streamer.core.hook;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.metrics.HdrHistogram;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mayconbordin
 */
public class TupleLatencyHdrHook extends Hook {
    private final HdrHistogram latencyTimer;

    public TupleLatencyHdrHook(HdrHistogram latencyTimer) {
        this.latencyTimer = latencyTimer;
    }

    @Override
    public void beforeTuple(Tuple tuple) {}
    
    @Override
    public void onEmit(Values values) {}

    @Override
    public void afterTuple(Tuple tuple) {
        if (tuple != null) {
            long latency = System.currentTimeMillis() - tuple.getLineageBirth();
            latencyTimer.update(latency, TimeUnit.MILLISECONDS);
        }
    }
}
