package com.streamer.examples.clickanalytics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.clickanalytics.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class VisitStatsOperator extends BaseOperator {
    private long total = 0;
    private long uniqueCount = 0;

    public void process(Tuple tuple) {
        boolean unique = Boolean.parseBoolean(tuple.getString(Field.UNIQUE));
        total++;
        if(unique) uniqueCount++;
        
        emit(tuple, new Values(total, uniqueCount));

    }
}
