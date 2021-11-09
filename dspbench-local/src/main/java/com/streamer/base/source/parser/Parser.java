package com.streamer.base.source.parser;

import com.streamer.core.Values;
import com.streamer.utils.Configuration;
import java.util.List;

/**
 *
 * @author mayconbordin
 */
public abstract class Parser {
    protected Configuration config;
    
    public void initialize(Configuration config) {
        this.config = config;
    }
    
    public List<Values> parse(String filename, String str) {
        return parse(str);
    }

    public abstract List<Values> parse(String str);
}
