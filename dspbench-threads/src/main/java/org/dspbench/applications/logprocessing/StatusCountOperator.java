package org.dspbench.applications.logprocessing;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.logprocessing.LogProcessingConstants.Field;
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
