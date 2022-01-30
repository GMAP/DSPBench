package org.dspbench.base.sink;

import org.dspbench.core.Component;
import org.dspbench.core.Sink;
import org.dspbench.base.constants.BaseConstants.BaseConfig;
import org.dspbench.base.sink.formatter.BasicFormatter;
import org.dspbench.base.sink.formatter.Formatter;
import org.dspbench.base.constants.BaseConstants;
import org.dspbench.utils.ClassLoaderUtils;
import org.dspbench.utils.Configuration;
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
