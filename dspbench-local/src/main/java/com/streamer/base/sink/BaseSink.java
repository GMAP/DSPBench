package com.streamer.base.sink;

import com.streamer.core.Component;
import com.streamer.core.Sink;
import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.sink.formatter.BasicFormatter;
import com.streamer.base.sink.formatter.Formatter;
import com.streamer.base.constants.BaseConstants;
import com.streamer.utils.ClassLoaderUtils;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;

/**
 *
 * @author mayconbordin
 */
public abstract class BaseSink extends Sink {
    private String configPrefix = BaseConstants.BASE_PREFIX;
    protected Formatter formatter;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        
        String formatterClass = config.getString(getConfigKey(BaseConfig.SINK_FORMATTER), null);
        
        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }
        
        formatter.initialize(config);
        
        initialize();
    }
    
    protected String getConfigKey(String template) {
        System.out.println("TEMPLATE = " + template + " / PREFIX = " + configPrefix + " / CLASS = " + this.getClass().getName());
        return String.format(template, configPrefix);
    }

    public void setConfigPrefix(String configPrefix) {
        System.out.println("PREFIX = " + configPrefix + " / CLASS = " + this.getClass().getName());
        this.configPrefix = configPrefix;
    }

    protected void initialize() {}
    protected abstract Logger getLogger();

    @Override
    public Component copy() {
        Component component = super.copy();
        ((BaseSink) component).setConfigPrefix(configPrefix);
        return component;
    }
}
