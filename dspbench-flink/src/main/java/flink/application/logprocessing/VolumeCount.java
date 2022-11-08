package flink.application.logprocessing;

import flink.util.Metrics;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class VolumeCount extends Metrics implements FlatMapFunction<Tuple7<Object, Object, Long, Object, Object, Object, String>, Tuple3<Long, Long, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeCount.class);

    private static CircularFifoBuffer buffer;
    private static Map<Long, MutableLong> counts;

    int windowSize = 60;

    Configuration config;

    public VolumeCount(Configuration config) {
        super.initialize(config);
        this.config = config;
        getBuffer();
        getCount();
    }

    private CircularFifoBuffer getBuffer() {
        if (buffer == null) {
            buffer = new CircularFifoBuffer(windowSize);
        }
        return buffer;
    }

    private Map<Long, MutableLong>  getCount() {
        if (counts == null) {
            counts = new HashMap<>(windowSize);
        }
        return counts;
    }

    @Override
    public void flatMap(Tuple7<Object, Object, Long, Object, Object, Object, String> input, Collector<Tuple3<Long, Long, String>> out) {
        super.initialize(config);
        getBuffer();
        getCount();

        long minute = input.getField(2);

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

        out.collect(new Tuple3<Long, Long, String>(minute, count.longValue(), input.f6));
        super.calculateThroughput();
    }
}
