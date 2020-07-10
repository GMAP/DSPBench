package com.streamer.examples.logprocessing;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.logprocessing.LogProcessingConstants.Config;
import com.streamer.examples.logprocessing.LogProcessingConstants.Field;
import com.streamer.examples.utils.DateUtils;
import com.streamer.utils.Configuration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt will count number of log events per minute
 */
public class VolumeCountOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(VolumeCountOperator.class);
    
    private CircularFifoBuffer buffer;
    private Map<Long, MutableLong> counts;

    @Override
    public void initialize() {
        int windowSize = config.getInt(Config.VOLUME_COUNTER_WINDOW, 60);
        
        buffer = new CircularFifoBuffer(windowSize);
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
