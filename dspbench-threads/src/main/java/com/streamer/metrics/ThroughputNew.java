package com.streamer.metrics;

import com.codahale.metrics.Gauge;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author mayconbordin
 */
public class ThroughputNew implements Gauge<Long> {
    private long startTime;
    private AtomicLong counter;    

    public ThroughputNew() {
        startTime = System.currentTimeMillis();
        counter  = new AtomicLong();
    }
    
    public void count() {
        counter.incrementAndGet();
    }

    public Long getValue() {
        long stopTime = System.currentTimeMillis();
        long count = counter.getAndSet(0);
        
        if (count <= 0) {
            startTime = stopTime;
            return 0L;
        }
        
        double seconds = (double)(stopTime - startTime)/1000.0;
        
        startTime = stopTime;
        
        return Math.round(count/seconds);
    }
}
