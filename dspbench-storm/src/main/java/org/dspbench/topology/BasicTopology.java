package org.dspbench.topology;

import org.apache.storm.Config;
import org.dspbench.spout.AbstractSpout;
import org.dspbench.constants.BaseConstants.BaseConf;
import org.dspbench.sink.BaseSink;

/**
 * The basic topology has only one spout and one sink, configured by the default
 * configuration keys.
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class BasicTopology extends AbstractTopology {
    protected AbstractSpout spout;
    protected BaseSink sink;
    protected int spoutThreads;
    protected int sinkThreads;
    
    public BasicTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        spout = loadSpout();
        sink  = loadSink();
        
        spoutThreads = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS), 1);
        sinkThreads  = config.getInt(getConfigKey(BaseConf.SINK_THREADS), 1);
    }
}
