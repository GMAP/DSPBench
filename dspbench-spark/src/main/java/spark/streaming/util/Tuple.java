package spark.streaming.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Tuple implements Serializable {
    private long createdAt;
    protected Map<String, Object> data;

    public Tuple() {
        this(System.currentTimeMillis());
    }

    public Tuple(long createdAt) {
        this.createdAt = createdAt;
        data = new HashMap<>();
    }
    
    public Tuple(Tuple parent) {
        this(parent.createdAt);
    }
    
    public Tuple(Tuple...parents) {
        this();
        
        for (Tuple p : parents) {
            if (p.createdAt < createdAt) {
                createdAt = p.createdAt;
            }
        }
    }
    
    public Tuple(List<Tuple> parents) {
        this();
        
        for (Tuple p : parents) {
            if (p.createdAt < createdAt) {
                createdAt = p.createdAt;
            }
        }
    }

    public void set(String key, Object val) {
        data.put(key, val);
    }

    public Object get(String key) {
        return data.get(key);
    }
    
    public String getString(String key) {
        return (String) data.get(key);
    }
    
    public Long getLong(String key) {
        return (Long) data.get(key);
    }
    
    public Integer getInt(String key) {
        return (Integer) data.get(key);
    }
    
    public Float getFloat(String key) {
        return (Float) data.get(key);
    }
    
    public Double getDouble(String key) {
        return (Double) data.get(key);
    }
    
    public Boolean getBoolean(String key) {
        return (Boolean) data.get(key);
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "Tuple{" + "createdAt=" + createdAt + ", map=" + data + '}';
    }
}