package com.streamer.core;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author mayconbordin
 */
public class SchemaTest {
    
    public SchemaTest() {
    }

    /**
     * Test of asKey method, of class Schema.
     */
    @Test
    public void testAsKey() {
        Schema schema = new Schema().asKey("id");
        
        assertArrayEquals(new String[]{"id"}, schema.getKeys().toArray());
        assertEquals(schema.getFields().size(), 0);
    }

    /**
     * Test of allKey method, of class Schema.
     */
    @Test
    public void testAllKey() {
        Schema schema = new Schema("id", "name").allKey();
        
        assertArrayEquals(new String[]{"id", "name"}, schema.getKeys().toArray());
    }

    /**
     * Test of keys method, of class Schema.
     */
    @Test
    public void testKeys() {
        Schema schema = new Schema().keys("id", "name");
        assertArrayEquals(new String[]{"id", "name"}, schema.getKeys().toArray());
        assertArrayEquals(new String[]{"id", "name"}, schema.getFields().toArray());
    }

    /**
     * Test of fields method, of class Schema.
     */
    @Test
    public void testFields() {
        Schema schema = new Schema().fields("id", "name");
        assertArrayEquals(new String[]{"id", "name"}, schema.getFields().toArray());
        assertEquals(schema.getKeys().size(), 0);
    }
}
