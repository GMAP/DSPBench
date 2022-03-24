package org.dspbench.base.sink.formatter;

import org.dspbench.core.Tuple;
import org.dspbench.utils.Configuration;

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