package com.streamer.core.hook;

import com.streamer.core.Tuple;
import com.streamer.core.Values;
import java.io.Serializable;

/**
 *
 * @author mayconbordin
 */
public abstract class Hook implements Serializable {
    public static enum Priority {
        High, Low
    }
    
    private Priority priority;

    public Hook() {
        priority = Priority.Low;
    }

    public Hook(Priority priority) {
        this.priority = priority;
    }
    
    public void beforeTuple(Tuple tuple){}
    public void afterTuple(Tuple tuple){}
    public void onEmit(Values values){}
    
    /**
     * Called when the source operator receives a new value.
     * @param value The raw value received
     */
    public void onSourceReceive(Object value){}

    public Priority getPriority() {
        return priority;
    }

    public void setPriority(Priority priority) {
        this.priority = priority;
    }
    
    public void setLowPriority() {
        this.priority = Priority.Low;
    }
    
    public void setHighPriority() {
        this.priority = Priority.High;
    }
    
    public boolean isLowPriority() {
        return (this.priority == Priority.Low);
    }
    
    public boolean isHighPriority() {
        return (this.priority == Priority.High);
    }
}
