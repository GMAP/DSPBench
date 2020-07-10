package com.streamer.base.source.generator;

import com.streamer.core.Values;
import com.streamer.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Generator {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }
    
    public abstract Values generate();

    public boolean hasNext() {
        return true;
    }
}