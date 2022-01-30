package org.dspbench.core;

import java.io.Serializable;
import java.util.ArrayList;

/**
 *
 * @author mayconbordin
 */
public class Values extends ArrayList<Serializable> {
    private long id;
    private String streamId = Constants.DEFAULT_STREAM;
    private transient Object tempValue;
    
    public Values() {
    }
    
    /*public Values(Serializable... vals) {
        super(vals.length);
        addAll(Arrays.asList(vals));
    }*/
    
    public Values(Object... objs) {
        super(objs.length);
        
        for (Object obj : objs) {
            add((Serializable)obj);
        }
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Object getTempValue() {
        return tempValue;
    }

    public void setTempValue(Object tempValue) {
        this.tempValue = tempValue;
    }
}
