package org.dspbench.base.task;

import org.dspbench.base.constants.BaseConstants.BaseConfig;
import org.dspbench.base.sink.BaseSink;
import org.dspbench.base.source.BaseSource;
import org.dspbench.core.Task;
import org.dspbench.utils.ClassLoaderUtils;
import org.dspbench.topology.Topology;
import org.dspbench.topology.TopologyBuilder;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractTask implements Task {
    protected TopologyBuilder builder;
    protected Configuration config;
    
    public void setTopologyBuilder(TopologyBuilder builder) {
        this.builder = builder;
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }

    public Topology getTopology() {
        return builder.build();
    }

    protected BaseSource loadSource() {
        return loadSource(BaseConfig.SOURCE_CLASS, getConfigPrefix());
    }
    
    protected BaseSource loadSource(String name) {
        return loadSource(BaseConfig.SOURCE_CLASS, String.format("%s.%s", getConfigPrefix(), name));
    }
    
    protected BaseSource loadSource(String configKey, String configPrefix) {
        String sourceClass = config.getString(String.format(configKey, configPrefix));
        BaseSource source = (BaseSource) ClassLoaderUtils.newInstance(sourceClass, "source", getLogger());
        source.setConfigPrefix(configPrefix);
        
        return source;
    }
    
    protected BaseSink loadSink() {
        return loadSink(BaseConfig.SINK_CLASS, getConfigPrefix());
    }
    
    protected BaseSink loadSink(String name) {
        return loadSink(BaseConfig.SINK_CLASS, String.format("%s.%s", getConfigPrefix(), name));
    }
    
    protected BaseSink loadSink(String configKey, String configPrefix) {
        String sinkClass = config.getString(String.format(configKey, configPrefix));
        BaseSink sink = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        sink.setConfigPrefix(configPrefix);
        
        return sink;
    }
    
    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     * @param key The configuration key to be parsed
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected String getConfigKey(String key, String name) {
        return String.format(key, String.format("%s.%s", getConfigPrefix(), name));
    }
    
    /**
     * Utility method to parse a configuration key with the application prefix..
     * @param key The configuration key to be parsed
     * @return 
     */
    protected String getConfigKey(String key) {
        return String.format(key, getConfigPrefix());
    }
    
    public abstract Logger getLogger();
    public abstract String getConfigPrefix();
}
