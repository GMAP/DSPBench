package com.streamer.core;

import java.util.Date;
import java.util.List;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author mayconbordin
 */
public class StreamTest {
    private static final int COMPONENT_ID = 100;
    private static final String COMPONENT_NAME = "component";
    
    private static Stream stream;
    private static Schema schema;
    private static Component component;
    
    @BeforeClass
    public static void setUpClass() {
        schema = new Schema();
        schema.keys("id").fields("name", "date");
        
        stream = new Stream("stream", schema) {
            @Override
            public void put(Component component, Tuple tuple) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        };
        
        component = mock(Component.class);
        when(component.getId()).thenReturn(COMPONENT_ID);
        when(component.getName()).thenReturn(COMPONENT_NAME);
    }
    
    @Test
    public void createTupleNoParent() {
        Values values = new Values(1, "test", new Date());
        values.setId(1);
        values.setStreamId("stream");
        
        Tuple tuple = stream.createTuple(component, null, values);
        
        assertTrue(tuple.componentId == COMPONENT_ID);
        assertTrue(tuple.componentName.equals(COMPONENT_NAME));
        assertTrue(tuple.id == 1);
        assertTrue(tuple.streamId.equals("stream"));
        
        List<String> fields = schema.getFields();
        
        assertTrue(fields.size() == values.size());
        
        for (int i=0; i<fields.size(); i++) {
            assertTrue(tuple.get(fields.get(i)) == values.get(i));
        }
    }
    
    @Test
    public void createTupleWithParent() {
        Values values = new Values(1, "test", new Date());
        values.setId(1);
        values.setStreamId("stream");
        
        Tuple parent = new Tuple();
        parent.id = 123;
        
        Tuple tuple = stream.createTuple(component, parent, values);
        
        assertTrue(tuple.componentId == COMPONENT_ID);
        assertTrue(tuple.componentName.equals(COMPONENT_NAME));
        assertTrue(tuple.id == parent.id);
        assertTrue(tuple.streamId.equals("stream"));
        
        List<String> fields = schema.getFields();
        
        assertTrue(fields.size() == values.size());
        
        for (int i=0; i<fields.size(); i++) {
            assertTrue(tuple.get(fields.get(i)) == values.get(i));
        }
    }
    
    @Test(expected=RuntimeException.class)
    public void createTupleNullValues() {
        stream.createTuple(component, null, null);
    }
    
    @Test(expected=RuntimeException.class)
    public void createTupleWrongValuesFormat() {
        Values values = new Values(1, "test");
        stream.createTuple(component, null, values);
    }
    
    @Test
    public void testTupleToList() {
        Tuple tuple = new Tuple();
        tuple.put("id", 1);
        tuple.put("name", "test");
        tuple.put("date", new Date());
        
        List<Object> list = stream.tupleToList(tuple);
        int count =0;
        for (String field : schema.getKeys()) {
            assertTrue(tuple.get(field).equals(list.get(count++)));
        }
        
        assertTrue(list.get(count).equals(tuple));
    }
}
