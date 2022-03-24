package org.dspbench.base.operator;

import org.dspbench.base.constants.BaseConstants;
import org.dspbench.core.Component;
import org.dspbench.core.Operator;
import org.dspbench.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public abstract class BaseOperator extends Operator {
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        initialize();
    }
    
    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
    
    protected void initialize() {}

    @Override
    public Component copy() {
        Component newInstance = super.copy();
        ((BaseOperator) newInstance).setConfigPrefix(configPrefix);

        return newInstance;
    }
}
