package com.streamer.base.sink;

import com.streamer.core.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class NullSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(NullSink.class);
    
    public void process(Tuple tuple) {
        // do nothing
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
}
