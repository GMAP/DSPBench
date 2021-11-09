package com.streamer.core.hook;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.codahale.metrics.Histogram;
import com.streamer.core.Tuple;
import com.streamer.core.Values;

/**
 *
 * @author mayconbordin
 */
public class TupleSizeHook extends Hook {
    private Histogram histogram;

    public TupleSizeHook(Histogram histogram) {
        this.histogram = histogram;
    }
    
    public void beforeTuple(Tuple tuple) {
    }

    public void afterTuple(Tuple tuple) {
    }

    public void onEmit(Values values) {
        long size = 0;
        
        for (Object o : values) {
            size += RamUsageEstimator.sizeOf(o);
        }
        
        histogram.update(size);
    }
    
}
