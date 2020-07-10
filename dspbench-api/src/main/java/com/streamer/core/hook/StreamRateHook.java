package com.streamer.core.hook;

import com.google.common.util.concurrent.RateLimiter;
import com.streamer.core.Tuple;
import com.streamer.core.Values;

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
