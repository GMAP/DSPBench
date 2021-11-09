package org.dspbench.source.generator;

import org.dspbench.util.config.Configuration;
import org.dspbench.util.stream.StreamValues;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Generator {
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }
    
    public abstract StreamValues generate();
}
