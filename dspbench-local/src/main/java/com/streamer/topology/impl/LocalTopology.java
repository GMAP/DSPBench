package com.streamer.topology.impl;

import com.streamer.topology.Topology;
import com.streamer.utils.Configuration;

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
