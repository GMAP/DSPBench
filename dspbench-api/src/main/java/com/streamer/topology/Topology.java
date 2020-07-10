package com.streamer.topology;

import com.streamer.core.Stream;
import com.streamer.utils.Configuration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Topology {
    protected String name;
    protected Map<String, Stream> streams;
    protected Map<String, IComponentAdapter> components;
    protected Configuration config;

    public Topology(String name, Configuration config) {
        this.name   = name;
        this.config = config;
        
        streams = new HashMap<String, Stream>();
        components = new HashMap<String, IComponentAdapter>();
    }
    
    public void addComponent(String name, IComponentAdapter component) {
        components.put(name, component);
    }
    
    public void addStream(String name, Stream stream) {
        streams.put(name, stream);
    }
    
    public IComponentAdapter getComponent(String name) {
        return components.get(name);
    }
    
    public Stream getStream(String name) {
        return streams.get(name);
    }
    
    public Collection<Stream> getStreams() {
        return streams.values();
    }
    
    public String getName() {
        return name;
    }

    public Configuration getConfiguration() {
        return config;
    }
    
    public List<IOperatorAdapter> getOperators() {
        List<IOperatorAdapter> list = new ArrayList<IOperatorAdapter>();
        
        for (IComponentAdapter adapter : components.values()) {
            if (adapter instanceof IOperatorAdapter)
                list.add((IOperatorAdapter) adapter);
        }
        
        return list;
    }
    
    public List<ISourceAdapter> getSources() {
        List<ISourceAdapter> list = new ArrayList<ISourceAdapter>();
        
        for (IComponentAdapter adapter : components.values()) {
            if (adapter instanceof ISourceAdapter)
                list.add((ISourceAdapter) adapter);
        }
        
        return list;
    }
    
    public List<IComponentAdapter> getComponents() {
        return new ArrayList<IComponentAdapter>(components.values());
    }
    
    public abstract void finish();
}
