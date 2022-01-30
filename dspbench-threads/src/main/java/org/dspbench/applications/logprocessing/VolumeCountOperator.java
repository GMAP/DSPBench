package org.dspbench.applications.logprocessing;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.logprocessing.LogProcessingConstants.Config;
import org.dspbench.applications.logprocessing.LogProcessingConstants.Field;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt will count number of log events per minute
 */
public class VolumeCountOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(VolumeCountOperator.class);
    
    private CircularFifoQueue<Long> buffer;
    private Map<Long, MutableLong> counts;

    @Override
    public void initialize() {
        int windowSize = config.getInt(Config.VOLUME_COUNTER_WINDOW, 60);
        
        buffer = new CircularFifoQueue(windowSize);
        counts = new HashMap<Long, MutableLong>(windowSize);
    }
    
    public void process(Tuple tuple) {
        long minute = tuple.getLong(Field.TIMESTAMP_MINUTES);
        
        MutableLong count = counts.get(minute);
        
        if (count == null) {
            if (buffer.isFull()) {
                long oldMinute = (Long) buffer.remove();
                counts.remove(oldMinute);
            }
            
            count = new MutableLong(1);
            counts.put(minute, count);
            buffer.add(minute);
        } else {
            count.increment();
        }
        
        emit(tuple, new Values(minute, count.longValue()));
    }
}
