package com.streamer.examples.utils;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;

/**
 *
 * @author mayconbordin
 */
public class DetailedConsoleSink extends Operator {

    public void process(Tuple tuple) {
        String info = String.format("{Stream=%s, Component=%s-%d, Tuple=%s", 
                tuple.getStreamId(), tuple.getComponentName(), tuple.getComponentId(), 
                tuple.getEntries());
        
        System.out.println(info);
    }
    
}
