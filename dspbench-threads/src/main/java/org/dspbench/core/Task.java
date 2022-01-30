package org.dspbench.core;

import org.dspbench.topology.Topology;
import org.dspbench.topology.TopologyBuilder;
import org.dspbench.utils.Configuration;

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
