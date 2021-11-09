package com.streamer.base.sink;

import com.streamer.core.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class LatencyFileSink extends AsyncFileSink {
    private static final Logger LOG = LoggerFactory.getLogger(LatencyFileSink.class);
    
    @Override
    public void process(Tuple tuple) {
        try {
            long latency = System.currentTimeMillis() - tuple.getLineageBirth();
            
            String values = tuple.getId() + "," + latency;
            
            queue.add(values);
        } catch (IllegalStateException ex) {
            LOG.warn("Queue is full", ex);
        }
    }
    
}
