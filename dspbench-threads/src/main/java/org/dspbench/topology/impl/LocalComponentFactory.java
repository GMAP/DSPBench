package org.dspbench.topology.impl;

import com.codahale.metrics.MetricRegistry;
import org.dspbench.core.Operator;
import org.dspbench.core.Source;
import org.dspbench.core.Stream;
import org.dspbench.core.Schema;
import org.dspbench.core.impl.LocalStream;
import org.dspbench.topology.ComponentFactory;
import org.dspbench.topology.IOperatorAdapter;
import org.dspbench.topology.ISourceAdapter;
import org.dspbench.topology.Topology;
import org.dspbench.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public class LocalComponentFactory implements ComponentFactory {
    private MetricRegistry metrics;
    private Configuration configuration;

    public void setMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public Stream createStream(String name, Schema schema) {
        return new LocalStream(name, schema);
    }

    public IOperatorAdapter createOperatorAdapter(String name, Operator operator) {
        LocalOperatorAdapter adapter = new LocalOperatorAdapter();
        adapter.setComponent(operator);
        adapter.setMetrics(metrics);
        adapter.setConfiguration(configuration);
        return adapter;
    }

    public ISourceAdapter createSourceAdapter(String name, Source source) {
        LocalSourceAdapter adapter = new LocalSourceAdapter();
        adapter.setComponent(source);
        adapter.setMetrics(metrics);
        adapter.setConfiguration(configuration);
        return adapter;
    }

    public Topology createTopology(String name) {
        return new LocalTopology(name, configuration);
    }
    
}
