package org.dspbench.topology.impl;

import org.dspbench.core.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalSourceInstance implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(LocalSourceInstance.class);
    private Source source;
    private int index;

    public LocalSourceInstance(Source source, int index) {
        this.source = source;
        this.index = index;
    }

    public Source getSource() {
        return source;
    }

    public int getIndex() {
        return index;
    }

    public void run() {
        while (source.hasNext()) {
            source.hooksBefore(null);
            source.nextTuple();
            source.hooksAfter(null);
        }
        
        LOG.info("Source {} finished", source.getDefaultOutputStream());
    }
}