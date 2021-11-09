package com.streamer.core.hook;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.metrics.ThroughputNew;

/**
 *
 * @author mayconbordin
 */
public class ThroughputHook extends Hook {
    private ThroughputNew throughput;
    //private Throughput.Context context;

    public ThroughputHook(ThroughputNew throughput) {
        this.throughput = throughput;
    }

    public void beforeTuple(Tuple tuple) {
        //context = throughput.time();
    }

    public void afterTuple(Tuple tuple) {
        //context.stop();
        throughput.count();
    }

    public void onEmit(Values values) {
    }
    
}
