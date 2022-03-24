package org.dspbench.base.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class NullSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(NullSource.class);

    @Override
    protected void initialize() {
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
}
