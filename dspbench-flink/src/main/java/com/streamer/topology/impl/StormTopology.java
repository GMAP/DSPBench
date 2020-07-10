package com.streamer.topology.impl;

import com.streamer.topology.Topology;
import com.streamer.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public class StormTopology extends Topology {
    
    public StormTopology(String name, Configuration config) {
        super(name, config);
    }

    @Override
    public void finish() {
        
    }
    
}
