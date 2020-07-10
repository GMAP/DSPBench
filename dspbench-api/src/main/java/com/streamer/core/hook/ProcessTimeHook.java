package com.streamer.core.hook;

import com.codahale.metrics.Timer;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import java.util.Random;

/**
 *
 * @author mayconbordin
 */
public class ProcessTimeHook extends Hook {
    private final Random rand = new Random();
    private final double sampleRate;
    private Timer timer;
    private Timer.Context context;
    
    public ProcessTimeHook(Timer timer, double sampleRate) {
        this.sampleRate = sampleRate;
        this.timer = timer;
    }

    @Override
    public void beforeTuple(Tuple tuple) {
        if (rand.nextDouble() < sampleRate) {
            context = timer.time();
        }
    }

    @Override
    public void afterTuple(Tuple tuple) {
        if (context != null) {
            context.stop();
            context = null;
        }
    }

    @Override
    public void onEmit(Values values) {
    }
    
}
