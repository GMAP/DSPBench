package org.dspbench.core.hook;

import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.metrics.ThroughputNew;

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
