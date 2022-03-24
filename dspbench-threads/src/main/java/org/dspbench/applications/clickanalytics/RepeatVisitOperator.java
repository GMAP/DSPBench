package org.dspbench.applications.clickanalytics;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.clickanalytics.ClickAnalyticsConstants.Field;

import java.util.HashMap;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class RepeatVisitOperator extends BaseOperator {
    private Map<String, Void> map;

    @Override
    public void initialize() {
        map = new HashMap<String, Void>();
    }

    public void process(Tuple tuple) {
        String clientKey = tuple.getString(Field.CLIENT_KEY);
        String url = tuple.getString(Field.URL);
        String key = url + ":" + clientKey;
        
        if (map.containsKey(key)) {
             emit(tuple, new Values(clientKey, Boolean.FALSE.toString()));
        } else {
            map.put(key, null);
            emit(tuple, new Values(clientKey, Boolean.TRUE.toString()));
        }
    }
}
