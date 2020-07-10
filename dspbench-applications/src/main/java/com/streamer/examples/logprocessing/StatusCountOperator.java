package com.streamer.examples.logprocessing;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.logprocessing.LogProcessingConstants.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountOperator  extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCountOperator.class);
    private Map<Integer, MutableLong> counts;

    @Override
    public void initialize() {
        counts = new HashMap<Integer, MutableLong>();
    }

    public void process(Tuple tuple) {
        int statusCode = tuple.getInt(Field.RESPONSE_CODE);
        MutableLong count = counts.get(statusCode);
        
        if (count == null) {
            count = new MutableLong();
            counts.put(statusCode, count);
        } else {
            count.increment();
        }

        emit(new Values(statusCode, count.getValue()));
    }
}
