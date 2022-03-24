package org.dspbench.base.source.generator;

import org.dspbench.core.Values;
import org.dspbench.utils.Configuration;

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