package org.dspbench.bolt;

import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

import org.dspbench.constants.BaseConstants;
import org.dspbench.hooks.BoltMeterHook;
import org.dspbench.util.config.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    
    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    protected Map<String, Fields> fields;
    
    public AbstractBolt() {
        fields = new HashMap<>();
    }
    
    public void setFields(Fields fields) {
        this.fields.put(BaseConstants.BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }
    
    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields.isEmpty()) {
            if (getDefaultFields() != null)
                fields.put(BaseConstants.BaseStream.DEFAULT, getDefaultFields());
            
            if (getDefaultStreamFields() != null)
                fields.putAll(getDefaultStreamFields());
        }
        
        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }
    
    public Fields getDefaultFields() {
        return null;
    }
    
    public Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config    = Configuration.fromMap(stormConf);
        this.context   = context;
        this.collector = collector;
        
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            context.addTaskHook(getMeterHook());
        }
        
        initialize();
    }

    public void initialize() {
        
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
    
    protected ITaskHook getMeterHook() {
        return new BoltMeterHook();
    }
}
