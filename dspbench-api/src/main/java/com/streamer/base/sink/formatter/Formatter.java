package com.streamer.base.sink.formatter;

import com.streamer.core.Tuple;
import com.streamer.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public abstract class Formatter {
    protected Configuration config;
    
    public void initialize(Configuration config) {
        this.config = config;
    }
    
    public abstract String format(Tuple tuple);
}