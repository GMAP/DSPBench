package org.dspbench.base.sink.formatter;

import org.dspbench.core.Tuple;
import java.io.Serializable;
import java.util.Map;

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