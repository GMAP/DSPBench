package org.dspbench.core.hook;

import com.codahale.metrics.Timer;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mayconbordin
 */
public class TupleLatencyHook extends Hook {
    private final Timer latencyTimer;

    public TupleLatencyHook(Timer latencyTimer) {
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
