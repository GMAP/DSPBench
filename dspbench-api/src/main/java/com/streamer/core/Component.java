package com.streamer.core;

import static com.streamer.core.Constants.DEFAULT_STREAM;
import com.streamer.core.hook.Hook;
import com.streamer.utils.Configuration;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class Component implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(Component.class);
    
    protected int id;
    protected String name;
    protected int parallelism;
    protected Map<String, Stream> outputStreams;
    protected Configuration config;
    
    protected List<Hook> highHooks;
    protected List<Hook> lowHooks;

    public Component() {
        outputStreams = new HashMap<String, Stream>();
        
        highHooks = new ArrayList<Hook>();
        lowHooks  = new ArrayList<Hook>();
    }

    public void onCreate(int id, Configuration config) {
        this.id     = id;
        this.config = config;
        
        LOG.info("started component: class={}, name={}, id={}", this.getClass().getSimpleName(), name, id);
    }

    public void onDestroy() { }
    
    
    // Emit functions ----------------------------------------------------------
    protected void emit(Values values) {
        emit(DEFAULT_STREAM, null, values);
    }
    
    protected void emit(Tuple parent, Values values) {
        emit(DEFAULT_STREAM, parent, values);
    }
    
    protected void emit(String stream, Values values) {
        emit(stream, null, values);
    }
    
    protected void emit(String stream, Tuple parent, Values values) {
        hooksOnEmit(values);
        
        if (outputStreams.containsKey(stream)) {
            if (parent == null) {
                outputStreams.get(stream).put(this, values);
            } else {
                outputStreams.get(stream).put(this, parent, values);
            }
            
        } else {
            LOG.error("Stream {} not found at component {}, valid streams: {}.", stream, getFullName(), outputStreams.toString());
        }
    }
    
    
    // Output Streams ----------------------------------------------------------
    public void addOutputStream(String id, Stream stream) {
        if (!outputStreams.containsKey(id)) {
            outputStreams.put(id, stream);
        }
    }
    
    public void addOutputStream(Stream stream) {
        if (!outputStreams.containsKey(DEFAULT_STREAM)) {
            outputStreams.put(DEFAULT_STREAM, stream);
        }
    }
    
    public Map<String, Stream> getOutputStreams() {
        return outputStreams;
    }
    
    public Stream getDefaultOutputStream() {
        if (outputStreams.containsKey(DEFAULT_STREAM))
            return outputStreams.get(DEFAULT_STREAM);
        else if (!outputStreams.isEmpty())
            return (Stream) outputStreams.values().toArray()[0];
        else
            return null;
    }
    
    public void setOutputStreams(Map<String, Stream> outputStreams) {
        this.outputStreams = outputStreams;
    }
    
    
    // Hooks -------------------------------------------------------------------
    public void addHook(Hook hook) {
        if (hook.isHighPriority())
            highHooks.add(hook);
        else
            lowHooks.add(hook);
    }

    public void addHooks(List<Hook> hooks) {
        for (Hook hook : hooks)
            addHook(hook);
    }
    
    public void hooksOnEmit(Values values) {
        for (Hook hook : lowHooks)
            hook.onEmit(values);

        for (Hook hook : highHooks)
            hook.onEmit(values);
    }
    
    public void hooksBefore(Tuple tuple) {
        for (Hook hook : lowHooks)
            hook.beforeTuple(tuple);
        
        for (Hook hook : highHooks)
            hook.beforeTuple(tuple);
    }
    
    public void hooksAfter(Tuple tuple) {
        for (Hook hook : highHooks)
            hook.afterTuple(tuple);
        
        for (Hook hook : lowHooks)
            hook.afterTuple(tuple);
    }
    
    public void hooksOnReceive(Object value) {
        for (Hook hook : highHooks)
            hook.onSourceReceive(value);
        
        for (Hook hook : lowHooks)
            hook.onSourceReceive(value);
    }
    
    public void resetHooks() {
        highHooks = new ArrayList<Hook>();
        lowHooks  = new ArrayList<Hook>();
    }
    
    // Accessors ---------------------------------------------------------------
    public List<Hook> getHooks() {
        List<Hook> hooks = new ArrayList<Hook>();
        hooks.addAll(highHooks);
        hooks.addAll(lowHooks);
        return hooks;
    }

    public void setHooks(List<Hook> hooks) {
        resetHooks();
        addHooks(hooks);
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setId(int id) {
        this.id = id;
    }
    
    public int getId() {
        return id;
    }
    
    public String getFullName() {
        return String.format("%s-%d", name, id);
    }
    
    
    // Utils -------------------------------------------------------------------
    public Component newInstance() {
        try {
            return getClass().newInstance();
        } catch (InstantiationException ex) {
            LOG.error("Error while copying object", ex);
        } catch (IllegalAccessException ex) {
            LOG.error("Error while copying object", ex);
        }
        return null;
    }
    
    public Component copy() {
        Component newInstance = (Component) newInstance();
        
        if (newInstance != null) {
            newInstance.setName(name);
            newInstance.setParallelism(parallelism);
            newInstance.outputStreams = outputStreams;
            newInstance.highHooks = new ArrayList<Hook>(highHooks);
            newInstance.lowHooks = new ArrayList<Hook>(lowHooks);
        }
        
        return newInstance;
    }
}
