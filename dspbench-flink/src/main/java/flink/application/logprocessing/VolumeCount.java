package flink.application.logprocessing;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class VolumeCount extends RichFlatMapFunction<Tuple6<Object, Object, Long, Object, Object, Object>, Tuple2<Long, Long>> {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeCount.class);

    private static CircularFifoBuffer buffer;
    private static Map<Long, MutableLong> counts;

    int windowSize = 60;

    Configuration config;

    Metrics metrics = new Metrics();

    public VolumeCount(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
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
    public void flatMap(Tuple6<Object, Object, Long, Object, Object, Object> input, Collector<Tuple2<Long, Long>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
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

        out.collect(new Tuple2<Long, Long>(minute, count.longValue()));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
