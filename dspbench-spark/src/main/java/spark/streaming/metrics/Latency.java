package spark.streaming.metrics;

import com.codahale.metrics.Gauge;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mayconbordin
 */
public class Latency implements Gauge<List<Long>> {
    private List<Long> latencies = new ArrayList<>();
    
    public void update(Long latency, TimeUnit unit) {
        long value = unit.toMillis(latency);
        latencies.add(value);
    }
    
    @Override
    public List<Long> getValue() {
        List<Long> temp = latencies;
        latencies = new ArrayList<>();
        return temp;
    }
    
}
