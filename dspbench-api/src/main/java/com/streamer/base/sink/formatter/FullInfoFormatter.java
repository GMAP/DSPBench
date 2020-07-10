package com.streamer.base.sink.formatter;

import com.streamer.core.Tuple;

/**
 *
 * @author mayconbordin
 */
public class FullInfoFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        return tuple.toString();
    }
    
}