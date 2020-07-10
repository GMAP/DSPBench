package com.streamer.base.source;

import com.streamer.base.sink.BaseSink;
import com.streamer.core.Component;
import com.streamer.core.Source;
import com.streamer.base.constants.BaseConstants;
import com.streamer.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public abstract class BaseSource extends Source {
    private String configPrefix = BaseConstants.BASE_PREFIX;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        
        initialize();
    }
    
    public String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    protected abstract void initialize();

    @Override
    public Component copy() {
        Component component = super.copy();
        ((BaseSource) component).setConfigPrefix(configPrefix);
        return component;
    }
}
