package com.streamer.core.impl;

import com.streamer.core.Component;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.core.Tuple;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.impl.LocalOperatorAdapter;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class LocalStream extends Stream {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStream.class);
    
    private List<Subscriber> subscribers;
    private int counter = 0;
    
    public LocalStream(String id, Schema schema) {
        super(id, schema);
        
        subscribers = new ArrayList<Subscriber>();
    }

    public void put(Component component, Tuple tuple) {
        put(tuple, nextCounter());
    }
    
    private void put(Tuple input, int count) {
        LocalOperatorAdapter adapter;
        int parallelism;
        
        for (Subscriber s : subscribers) {
            adapter = (LocalOperatorAdapter) s.getAdapter();
            parallelism = adapter.getComponent().getParallelism();
            
            switch (s.getPartitioning()) {
                case SHUFFLE:
                    adapter.processTuple(input, count%parallelism);
                    break;
                    
                case BROADCAST:
                    for (int p = 0; p < parallelism; p++) {
                        adapter.processTuple(input, p);
                    }
                    break;
                
                case GROUP_BY:
                    StringBuilder sb = new StringBuilder();
                    if (s.getFields() != null) {
                        for (String key : s.getFields())
                            sb.append(input.get(key));
                    } else {
                        for (String key : schema.getKeys())
                            sb.append(input.get(key));
                    }
                    
                    adapter.processTuple(input, getIndexForKey(sb.toString(), parallelism));
                    break;
            }
            
        }
    }
    
    @Override
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning) {
        addSubscriber(name, op, partitioning, null);
    }

    @Override
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning, Fields fields) {
        subscribers.add(new Subscriber(name, op, partitioning, fields));
    }
    
    private int nextCounter() {
        if (counter >= Integer.MAX_VALUE) counter = 0;
        counter += 1;
        return counter;
    }
    
    private static int getIndexForKey(String key, int parallelism) {
        if (key == null) return 0;
        int index = HashCodeBuilder.reflectionHashCode(key, true) % parallelism;
        if (index < 0) {
            index += parallelism;
        }
        return index;
    }
    
    private static class Subscriber {
        private String name;
        private IOperatorAdapter adapter;
        private PartitioningScheme partitioning;
        private Fields fields;

        public Subscriber(String name, IOperatorAdapter adapter, PartitioningScheme partitioning, Fields fields) {
            this.name = name;
            this.adapter = adapter;
            this.partitioning = partitioning;
            this.fields = fields;
        }

        public String getName() {
            return name;
        }

        public IOperatorAdapter getAdapter() {
            return adapter;
        }

        public PartitioningScheme getPartitioning() {
            return partitioning;
        }

        public Fields getFields() {
            return fields;
        }
        
        
    }
}
