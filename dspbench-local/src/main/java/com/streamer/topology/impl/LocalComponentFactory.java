package com.streamer.topology.impl;

import com.codahale.metrics.MetricRegistry;
import com.streamer.core.Operator;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.core.Schema;
import com.streamer.core.impl.LocalStream;
import com.streamer.topology.ComponentFactory;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.ISourceAdapter;
import com.streamer.topology.Topology;
import com.streamer.utils.Configuration;

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
