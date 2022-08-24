package org.dspbench.applications.clickanalytics;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.bolt.AbstractBolt;

import java.util.HashMap;

import java.util.Map;
import static org.dspbench.applications.clickanalytics.ClickAnalyticsConstants.*;

/**
 * User: domenicosolazzo
 */
public class RepeatVisitBolt extends AbstractBolt {
    private Map<String, Void> map;

    @Override
    public void initialize() {
        map = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String clientKey = input.getStringByField(Field.CLIENT_KEY);
        String url = input.getStringByField(Field.URL);
        String key = url + ":" + clientKey;
        
        if (map.containsKey(key)) {
             collector.emit(input, new Values(clientKey, url, Boolean.FALSE.toString(), input.getStringByField(Field.INITTIME)));
        } else {
            map.put(key, null);
            collector.emit(input, new Values(clientKey, url, Boolean.TRUE.toString(), input.getStringByField(Field.INITTIME)));
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.CLIENT_KEY, Field.URL, Field.UNIQUE, Field.INITTIME);
    }
}
