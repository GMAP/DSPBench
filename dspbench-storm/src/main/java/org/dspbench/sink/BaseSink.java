package org.dspbench.sink;

import org.apache.storm.tuple.Fields;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.constants.BaseConstants;
import org.dspbench.sink.formatter.BasicFormatter;
import org.dspbench.sink.formatter.Formatter;
import org.dspbench.util.config.ClassLoaderUtils;
import org.slf4j.Logger;


public abstract class BaseSink extends AbstractBolt {
    protected Formatter formatter;
    
    @Override
    public void initialize() {
        String formatterClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_FORMATTER), null);
        
        if (formatterClass == null) {
            formatter = new BasicFormatter();
        } else {
            formatter = (Formatter) ClassLoaderUtils.newInstance(formatterClass, "formatter", getLogger());
        }
        
        formatter.initialize(config, context);
    }
    
    @Override
    public Fields getDefaultFields() {
        return new Fields("");
    }
    
    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }
    
    protected abstract Logger getLogger();
}
