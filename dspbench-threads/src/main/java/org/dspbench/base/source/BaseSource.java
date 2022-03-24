package org.dspbench.base.source;

import org.dspbench.core.Component;
import org.dspbench.core.Source;
import org.dspbench.base.constants.BaseConstants;
import org.dspbench.utils.Configuration;

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
