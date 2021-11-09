package com.streamer.core.hook;

import com.codahale.metrics.Counter;
import com.streamer.core.Tuple;
import com.streamer.core.Values;

/**
 *
 * @author mayconbordin
 */
public class TupleCounterHook extends Hook {
    private Counter inputCount;
    private Counter outputCount;

    public TupleCounterHook(Counter inputCount, Counter outputCount) {
        this.inputCount = inputCount;
        this.outputCount = outputCount;
    }

    @Override
    public void afterTuple(Tuple tuple) {
        inputCount.inc();
    }

    @Override
    public void onEmit(Values values) {
        outputCount.inc();
    }
    
}
