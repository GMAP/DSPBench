package com.streamer.core;

import com.streamer.topology.Topology;
import com.streamer.topology.TopologyBuilder;
import com.streamer.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface Task {
    public void setTopologyBuilder(TopologyBuilder builder);
    public void setConfiguration(Configuration config);
    
    public void initialize();
    public Topology getTopology();
}
