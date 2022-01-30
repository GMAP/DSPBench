package org.dspbench.topology.impl;

import org.dspbench.topology.Topology;
import org.dspbench.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public class LocalTopology extends Topology {

    public LocalTopology(String name, Configuration config) {
        super(name, config);
    }

    @Override
    public void finish() {
    }
}
