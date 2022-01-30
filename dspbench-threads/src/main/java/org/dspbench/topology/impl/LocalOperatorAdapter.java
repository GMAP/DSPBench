package org.dspbench.topology.impl;

import com.codahale.metrics.MetricRegistry;
import org.dspbench.core.Operator;
import org.dspbench.core.Tuple;
import org.dspbench.core.hook.Hook;
import org.dspbench.topology.IOperatorAdapter;
import org.dspbench.utils.Configuration;
import org.dspbench.metrics.MetricsFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class LocalOperatorAdapter implements IOperatorAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(LocalOperatorAdapter.class);
    private MetricRegistry metrics;
    private Configuration config;
    
    private Operator operator;
    private List<LocalOperatorInstance> instances;
    private List<Hook> hooks = new ArrayList<Hook>();
    private long timerInMillis = 0L;
    
    public void setComponent(Operator operator) {
        this.operator = operator;
    }

    public Operator getComponent() {
        return operator;
    }

    public void setTimeInterval(long interval, TimeUnit timeUnit) {
        timerInMillis = timeUnit.toMillis(interval);
    }
    
    public boolean hasTimer() {
        return timerInMillis > 0L;
    }
    
    public long getTimeIntervalMillis() {
        return timerInMillis;
    }
    
    public void processTuple(Tuple input, int instance) {
        //operator.process(input);
        instances.get(instance).processTuple(input);
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }
    
    public void setupInstances() {
        instances = new ArrayList<LocalOperatorInstance>();
        
        for (int p=0; p<operator.getParallelism(); p++) {
            Operator component = (Operator) operator.copy();
            component.onCreate(p, config);
            component.addHooks(hooks);
            
            if (metrics != null) {
                String name = component.getFullName();
                
                component.addHook(MetricsFactory.createThroughputHook(metrics, name));
                component.addHook(MetricsFactory.createTupleSizeHook(metrics, name));
                component.addHook(MetricsFactory.createTupleCounterHook(metrics, name));
                component.addHook(MetricsFactory.createProcessTimeHook(metrics, name));
            }
            
            LocalOperatorInstance instance = new LocalOperatorInstance(component, p);
            instances.add(p, instance);
        }
    }
    
    public List<LocalOperatorInstance> getInstances() {
        return instances;
    }

    public void setMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void addComponentHook(Hook hook) {
        hooks.add(hook);
    }
}
