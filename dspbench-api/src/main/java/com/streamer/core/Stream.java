package com.streamer.core;

import com.streamer.partitioning.PartitioningScheme;
import com.streamer.topology.IOperatorAdapter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Stream implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Stream.class);
    
    protected String id;
    protected Schema schema;
    
    public Stream(String id, Schema schema) {
        this.id = id;
        this.schema = schema;
    }

    public String getStreamId() {
        return id;
    }

    public Schema getSchema() {
        return schema;
    }

    public void put(Component component, Values values) {
        put(component, createTuple(component, null, values));
    }

    public void put(Component component, Tuple parent, Values values) {
        put(component, createTuple(component, parent, values));
    }
    
    public abstract void put(Component component, Tuple tuple);
    
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning) { }
    public void addSubscriber(String name, IOperatorAdapter op, PartitioningScheme partitioning, com.streamer.partitioning.Fields fields) { }
    public void addPublisher(Component component) { }
    
    protected Tuple createTuple(Component component, Tuple parent, Values values) {
        if (values == null)
            throw new RuntimeException("Received tuple is empty");
        
        if (values.size() != schema.getFields().size())
            throw new RuntimeException("Received tuple is not compliant with stream schema");
        
        long tupleId = (parent == null) ? values.getId() : parent.getId();
        
        Tuple tuple = new Tuple(parent);
        tuple.setId(tupleId);
        tuple.setComponentName(component.getName());
        tuple.setComponentId(component.getId());
        tuple.setStreamId(id);
        tuple.setTempValue(values.getTempValue());
        
        for (int i=0; i<values.size(); i++) {
            tuple.put(schema.getFields().get(i), values.get(i));
        }
        
        return tuple;
    }
    
    protected List<Object> tupleToList(Tuple tuple) {
        List<Object> orderedValues = new ArrayList<Object>();

        for (String field : schema.getKeys()) {
            orderedValues.add(tuple.get(field));
        }

        // add the tuple itself
        orderedValues.add(tuple);

        return orderedValues;
    }
}
