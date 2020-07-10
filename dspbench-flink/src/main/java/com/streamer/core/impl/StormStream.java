package com.streamer.core.impl;

import backtype.storm.topology.BoltDeclarer;
import com.streamer.core.Component;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.core.Tuple;
import com.streamer.partitioning.Fields;
import com.streamer.partitioning.PartitioningScheme;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.impl.StormOperatorAdapter;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class StormStream extends Stream {
    private List<String> publishers;

    public StormStream(String id, Schema schema) {
        super(id, schema);
        publishers = new ArrayList<String>();
    }

    @Override
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning) {
        addSubscriber(name, op, partitioning, null);
    }

    @Override
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning, Fields fields) {
        BoltDeclarer declarer = ((StormOperatorAdapter)op).getDeclarer();
        
        switch (partitioning) {
            case BROADCAST:
                for (String p : publishers) {
                    declarer.allGrouping(p, id);
                }
                break;
                
            case SHUFFLE:
                for (String p : publishers) {
                    declarer.shuffleGrouping(p, id);
                }
                break;
                
            case GROUP_BY:
                for (String p : publishers) {
                    if (fields == null) {
                        declarer.fieldsGrouping(p, id, new backtype.storm.tuple.Fields(schema.getKeys()));
                    } else {
                        declarer.fieldsGrouping(p, id, new backtype.storm.tuple.Fields(fields));
                    }
                }
                break;
        }
    }

    public void put(Component component, Tuple tuple) {
        // do nothing
        throw new RuntimeException("You should not have called this");
    }
    
    @Override
    public void addPublisher(Component component) {
        publishers.add(component.getName());
    }
}