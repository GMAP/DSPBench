package com.streamer.base.task;

import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.sink.BaseSink;
import com.streamer.base.source.BaseSource;
import com.streamer.utils.Configuration;

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
