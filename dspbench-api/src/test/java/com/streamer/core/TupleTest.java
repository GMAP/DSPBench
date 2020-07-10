package com.streamer.core;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * @author mayconbordin
 */
public class TupleTest {
    private static final String BIN_FILE = "/tmp/tuple.bin";
    private static final int TUPLE_ID = 100;
    private static final String TUPLE_STREAM_ID = "stream";
    private static final int TUPLE_COMPONENT_ID = 0;
    private static final String TUPLE_COMPONENT_NAME = "operator";
            
    private static Kryo kryo;
    private static Map<String, Serializable> map;
    
    @Rule
    public ExpectedException exception = ExpectedException.none();
    
    
    @BeforeClass
    public static void setUpClass() {
        kryo = new Kryo();
        kryo.register(Tuple.class, new Tuple.TupleSerializer());
        
        map = new HashMap<String, Serializable>();
        map.put("a", 1);
        map.put("b", 2);
        map.put("c", "test");
    }
    
    @Test
    public void withParent() {
        Tuple parent = new Tuple();
        parent.createdAt = System.currentTimeMillis();
        
        Tuple child = new Tuple(parent);
        
        assertEquals(child.lineageBirth/1000, parent.createdAt/1000);
    }
    
    @Test
    public void serialize() {
        Tuple tuple = new Tuple();
        tuple.setId(TUPLE_ID);
        tuple.setStreamId(TUPLE_STREAM_ID);
        tuple.setComponentId(TUPLE_COMPONENT_ID);
        tuple.setComponentName(TUPLE_COMPONENT_NAME);
        
        tuple.map = map;
        
        try {
            Output output = new Output(new FileOutputStream(BIN_FILE));
            kryo.writeObject(output, tuple);
            output.close();
        } catch (FileNotFoundException ex) {
            fail("Error: " + ex.getMessage());
        } catch (KryoException ex) {
            fail("Error: " + ex.getMessage());
        }
    }
    
    @Test
    public void deserialize() {
        try {
            Input input = new Input(new FileInputStream(BIN_FILE));
            Tuple tuple = kryo.readObject(input, Tuple.class);
            input.close();
            
            assertEquals(tuple.id, TUPLE_ID);
            assertEquals(tuple.componentId, TUPLE_COMPONENT_ID);
            assertEquals(tuple.componentName, TUPLE_COMPONENT_NAME);
            assertEquals(tuple.streamId, TUPLE_STREAM_ID);
            assertEquals(tuple.createdAt, tuple.lineageBirth);
            assertEquals(tuple.map, map);
        } catch (FileNotFoundException ex) {
            fail("Error: " + ex.getMessage());
        }
    }
    
    @Test
    public void typeCasting() {
        Tuple tuple = new Tuple();
        
        tuple.put("a", 10.5);
        tuple.getDouble("a");
        
        exception.expect(ClassCastException.class);
        tuple.getInt("a");
    }
}
