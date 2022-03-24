package org.dspbench.base.task;

import org.dspbench.base.constants.BaseConstants.BaseConfig;
import org.dspbench.base.sink.BaseSink;
import org.dspbench.base.source.BaseSource;
import org.dspbench.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public abstract class BasicTask extends AbstractTask {
    protected BaseSource source;
    protected BaseSink sink;
    
    protected int sourceRate;
    protected int sourceThreads;
    protected int sinkThreads;
    
    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        source = loadSource();
        sink   = loadSink();
        
        sourceThreads = config.getInt(getConfigKey(BaseConfig.SOURCE_THREADS), 1);
        sourceRate    = config.getInt(getConfigKey(BaseConfig.SOURCE_RATE), -1);
        sinkThreads   = config.getInt(getConfigKey(BaseConfig.SINK_THREADS), 1);
    }
    
}
