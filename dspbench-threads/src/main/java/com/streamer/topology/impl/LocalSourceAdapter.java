package com.streamer.topology.impl;

import com.codahale.metrics.MetricRegistry;
import com.streamer.core.Source;
import com.streamer.core.hook.Hook;
import com.streamer.core.hook.StreamRateHook;
import com.streamer.topology.ISourceAdapter;
import com.streamer.utils.Configuration;
import com.streamer.metrics.MetricsFactory;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class LocalSourceAdapter implements ISourceAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(LocalSourceAdapter.class);
    private MetricRegistry metrics;   
    private Configuration config;
    private Source source;
    private List<LocalSourceInstance> instances;
    private List<Hook> hooks = new ArrayList<Hook>();

    public void setComponent(Source source) {
        this.source = source;
    }

    public Source getComponent() {
        return source;
    }

    public void setTupleRate(int rate) {
        hooks.add(new StreamRateHook(rate));
    }

    public void setMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void setConfiguration(Configuration config) {
        this.config = config;
    }
    
    public void setupInstances() {
        instances = new ArrayList<LocalSourceInstance>();
        
        for (int p=0; p<source.getParallelism(); p++) {
            Source component = (Source) source.copy();
            component.onCreate(p, config);
            component.addHooks(hooks);
            
            if (metrics != null) {
                String name = component.getFullName();
                
                component.addHook(MetricsFactory.createThroughputHook(metrics, name));
                component.addHook(MetricsFactory.createTupleSizeHook(metrics, name));
                component.addHook(MetricsFactory.createTupleCounterHook(metrics, name));
                component.addHook(MetricsFactory.createProcessTimeHook(metrics, name));
            }
            
            LocalSourceInstance instance = new LocalSourceInstance(component, p);
            instances.add(p, instance);
        }
    }
    
    public List<LocalSourceInstance> getInstances() {
        return instances;
    }

    public void addComponentHook(Hook hook) {
        hooks.add(hook);
    }
}
