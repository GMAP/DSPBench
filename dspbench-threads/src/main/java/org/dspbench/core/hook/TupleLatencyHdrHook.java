package org.dspbench.core.hook;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.metrics.HdrHistogram;

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
