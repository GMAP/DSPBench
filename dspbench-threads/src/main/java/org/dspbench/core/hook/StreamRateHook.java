package org.dspbench.core.hook;

import com.google.common.util.concurrent.RateLimiter;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;

/**
 *
 * @author mayconbordin
 */
public class StreamRateHook extends Hook {
    private RateLimiter rateLimiter;

    public StreamRateHook(int rate) {
        rateLimiter = RateLimiter.create(rate);
    }

    public void beforeTuple(Tuple tuple) {
        rateLimiter.acquire();
    }

    public void afterTuple(Tuple tuple) {
    }

    public void onEmit(Values values) {
    }
}
