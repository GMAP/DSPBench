package org.dspbench.applications.clickanalytics;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.clickanalytics.ClickAnalyticsConstants.Field;

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
