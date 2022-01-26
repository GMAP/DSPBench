package storm.applications.bolt;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.LogProcessingConstants.Field;

/**
 * This bolt will count the status codes from http logs such as 200, 404, 503
 */
public class StatusCountBolt  extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StatusCountBolt.class);
    private Map<Integer, Integer> counts;

    @Override
    public void initialize() {
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        int statusCode = input.getIntegerByField(Field.RESPONSE);
        int count = 0;
        
        if (counts.containsKey(statusCode)) {
            count = counts.get(statusCode);
        }
        
        count++;
        counts.put(statusCode, count);
        
        collector.emit(input, new Values(statusCode, count));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.RESPONSE, Field.COUNT);
    }
}
