package com.streamer.base.sink.formatter;

import com.streamer.core.Tuple;
import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author mayconbordin
 */
public class BasicFormatter extends Formatter {

    @Override
    public String format(Tuple tuple) {
        StringBuilder sb = new StringBuilder();
        
        for (Map.Entry<String, Serializable> e : tuple.getEntries().entrySet()) {
            sb.append(String.format("%s=%s;", e.getKey(), e.getValue()));
        }
        
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }
    
}