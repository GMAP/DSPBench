package org.dspbench.core.hook;

import com.codahale.metrics.Timer;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

/**
 *
 * @author mayconbordin
 */
public class TimerHook extends Hook {
    private Timer timer;
    private Timer.Context context;

    public TimerHook(Timer timer) {
        this.timer = timer;
    }

    public void beforeTuple(Tuple tuple) {
        context = timer.time();
    }

    public void afterTuple(Tuple tuple) {
        context.stop();
    }

    public void onEmit(Values values) {
    }
    
    
}
