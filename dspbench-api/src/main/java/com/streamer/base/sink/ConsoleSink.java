package com.streamer.base.sink;

import com.streamer.core.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    
    public void process(Tuple tuple) {
        System.out.println(formatter.format(tuple));
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
}
