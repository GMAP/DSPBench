package org.dspbench.base.sink.formatter;

import org.dspbench.core.Tuple;

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