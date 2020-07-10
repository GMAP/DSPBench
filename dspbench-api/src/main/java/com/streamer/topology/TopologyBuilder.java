package com.streamer.topology;

import com.streamer.core.Operator;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class TopologyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(TopologyBuilder.class);
    
    private Topology topology;
    private ComponentFactory factory;

    public void initTopology(String name) {
        topology = factory.createTopology(name);
    }
    
    public Topology build() {
        topology.finish();
        return topology;
    }

    public Stream createStream(String name, Schema schema) {
        LOG.info("Creating stream '{}' with {}", name, schema.toString());
        Stream stream = (Stream) factory.createStream(name, schema);
        topology.addStream(name, stream);
        
        return stream;
    }
    
    public void setSource(String name, Source source) {
        setSource(name, source, 1);
    }
    
    public void setSource(String name, Source source, int parallelism) {
        LOG.info("Set source '{}' with {} instances", name, parallelism);
        source.setName(name);
        source.setParallelism(parallelism);
        
        ISourceAdapter adapter = factory.createSourceAdapter(name, source);
        topology.addComponent(name, adapter);
    }
    
    public void setOperator(String name, Operator op) {
        setOperator(name, op, 1);
    }
    
    public void setOperator(String name, Operator op, int parallelism) {
        LOG.info("Set operator '{}' with {} instances", name, parallelism);
        op.setName(name);
        op.setParallelism(parallelism);

        IOperatorAdapter adapter = factory.createOperatorAdapter(name, op);
        topology.addComponent(name, adapter);
    }
    
    public void publish(String component, Stream...targets) {
        IComponentAdapter adapter = topology.getComponent(component);
        
        if (adapter == null)
            throw new IllegalArgumentException("Component does not exists");
        
        for (Stream target : targets) {
            LOG.info("Component '{}' publishing at '{}' stream", component, target.getStreamId());
            adapter.getComponent().addOutputStream(target);
            adapter.getComponent().addOutputStream(target.getStreamId(), target);
            target.addPublisher(adapter.getComponent());
        }
    }
    
    public void shuffle(String component, Stream source) {
        subscribe(component, source, PartitioningScheme.SHUFFLE);
    }
    
    public void bcast(String component, Stream source) {
        subscribe(component, source, PartitioningScheme.BROADCAST);
    }
    
    public void groupBy(String component, Stream source, Fields fields) {
        subscribe(component, source, PartitioningScheme.GROUP_BY, fields);
    }
    
    public void groupByKey(String component, Stream source) {
        subscribe(component, source, PartitioningScheme.GROUP_BY, null);
    }

    public void subscribe(String component, Stream source, PartitioningScheme partitioning) {
        subscribe(component, source, partitioning, null);
    }
    
    public void subscribe(String component, Stream source, PartitioningScheme partitioning, Fields fields) {
        LOG.info("Component '{}' subscribing ({}) to '{}' stream", component, partitioning.toString(), source.getStreamId());
        IComponentAdapter adapter = topology.getComponent(component);
        
        if (adapter == null)
            throw new IllegalArgumentException("Component does not exists");
        
        if (adapter instanceof IOperatorAdapter) {
            source.addSubscriber(component, (IOperatorAdapter) adapter, partitioning, fields);
            ((IOperatorAdapter)adapter).getComponent().addInputStream(source);
        }
    }
    
    public void setTupleRate(String component, int rate) {
        if (rate < 1) return; // unlimited
        LOG.info("Source '{}' limited to {} tuples/sec", component, rate);
        IComponentAdapter adapter = topology.getComponent(component);
        
        if (adapter == null)
            throw new IllegalArgumentException("Component does not exists");
        
        if (adapter instanceof ISourceAdapter)
            ((ISourceAdapter)adapter).setTupleRate(rate);
        else
            throw new IllegalArgumentException("Component is not a Source");
    }
    
    public void setTimer(String component, long interval, TimeUnit timeUnit) {
        LOG.info("Setting timer for '{}' operator", component);
        IComponentAdapter adapter = topology.getComponent(component);
        
        if (adapter == null)
            throw new IllegalArgumentException("Component does not exists");
        
        if (adapter instanceof IOperatorAdapter)
            ((IOperatorAdapter)adapter).setTimeInterval(interval, timeUnit);
    }

    public void setFactory(ComponentFactory factory) {
        this.factory = factory;
    }
}
