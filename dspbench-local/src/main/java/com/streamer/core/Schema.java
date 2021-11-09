package com.streamer.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author mayconbordin
 */
public class Schema implements Serializable {
    private List<String> keys;
    private List<String> fields;
    
    public Schema() {
        this(new ArrayList<String>(), new ArrayList<String>());
    }
    
    public Schema(List<String> fields) {
        this(fields, new ArrayList<String>());
    }
    
    public Schema(String...fields) {
        this(Arrays.asList(fields), new ArrayList<String>());
    }
    
    public Schema(List<String> fields, List<String> keys) {
        this.fields = fields;
        this.keys = keys;
    }
    
    /**
     * Set the list of field names as key.
     * @param k List of fields
     * @return 
     */
    public Schema asKey(String...k) {
        keys.addAll(Arrays.asList(k));
        return this;
    }
    
    /**
     * Set all fields of the schema as key.
     * @return 
     */
    public Schema allKey() {
        keys.addAll(fields);
        return this;
    }
        
    /**
     * Add all field names as both key and field.
     * @param k List of fields
     * @return 
     */
    public Schema keys(String...k) {
        keys.addAll(Arrays.asList(k));
        fields.addAll(Arrays.asList(k));
        return this;
    }
    
    /**
     * Add all field names as fields.
     * @param k List of fields
     * @return 
     */
    public Schema fields(String...k) {
        fields.addAll(Arrays.asList(k));
        return this;
    }
    
    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(String...keys) {
        this.keys = Arrays.asList(keys);
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(String...fields) {
        this.fields = Arrays.asList(fields);
    }

    @Override
    public String toString() {
        return "Schema{" + "keys=" + keys + ", fields=" + fields + '}';
    }
}
