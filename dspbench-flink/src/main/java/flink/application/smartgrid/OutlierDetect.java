package flink.application.smartgrid;

import flink.application.trafficmonitoring.collections.FixedMap;
import flink.constants.SmartGridConstants;
import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class OutlierDetect extends RichCoFlatMapFunction<Tuple4<String, String, Long, Double>,Tuple4<String, String, Long, Double>, Tuple4<Long, Long, String, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(OutlierDetect.class);

    private static FixedMap<Long, Double> globalMedianBacklog;
    private static Map<String, OutlierTracker> outliers;
    private static PriorityQueue<ComparableTuple> unprocessedMessages;

    Configuration config;

    Metrics metrics = new Metrics();

    public OutlierDetect(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    private FixedMap<Long, Double>  med() {
        if (globalMedianBacklog == null) {
            globalMedianBacklog = new FixedMap<Long, Double>(300,300);
        }

        return globalMedianBacklog;
    }

    private Map<String, OutlierTracker>  outl() {
        if (outliers == null) {
            outliers = new HashMap<>();
        }

        return outliers;
    }

    private PriorityQueue<ComparableTuple>  msg() {
        if (unprocessedMessages == null) {
            unprocessedMessages = new PriorityQueue<>();
        }

        return unprocessedMessages;
    }

    @Override
    public void flatMap1(Tuple4<String, String, Long, Double> input, Collector<Tuple4<Long, Long, String, Double>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        med();
        outl();
        msg();

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String component = input.getField(0);

        if (component.equals(SmartGridConstants.Component.GLOBAL_MEDIAN)) {
            long timestamp = input.getField(2);
            double globalMedianLoad = input.getField(3);

            globalMedianBacklog.put(timestamp, globalMedianLoad);

            // ordered based on the timestamps
            while (!unprocessedMessages.isEmpty() && unprocessedMessages.peek().tuple.getField(2).equals(timestamp)) {
                Tuple perPlugMedianTuple = unprocessedMessages.poll().tuple;
                Tuple4<Long, Long, String, Double> dados = processPerPlugMedianTuple(perPlugMedianTuple);
                if (!(dados == null)){
                    if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                        metrics.emittedThroughput();
                    }
                    out.collect(new Tuple4<>(dados.f0, dados.f1, dados.f2, dados.f3));
                }
            }
        } else {
            processPerPlugMedianTuple(input);
        }
    }

    @Override
    public void flatMap2(Tuple4<String, String, Long, Double> input, Collector<Tuple4<Long, Long, String, Double>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        med();
        outl();
        msg();

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String component = input.getField(0);

        if (component.equals(SmartGridConstants.Component.GLOBAL_MEDIAN)) {
            long timestamp = input.getField(2);
            double globalMedianLoad = input.getField(3);

            globalMedianBacklog.put(timestamp, globalMedianLoad);

            // ordered based on the timestamps
            while (!unprocessedMessages.isEmpty() && unprocessedMessages.peek().tuple.getField(2).equals(timestamp)) {
                Tuple perPlugMedianTuple = unprocessedMessages.poll().tuple;
                Tuple4<Long, Long, String, Double> dados = processPerPlugMedianTuple(perPlugMedianTuple);
                if (!(dados == null)){
                    if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                        metrics.emittedThroughput();
                    }
                    out.collect(new Tuple4<>(dados.f0, dados.f1, dados.f2, dados.f3));
                }
            }
        } else {
            processPerPlugMedianTuple(input);
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    private Tuple4<Long, Long, String, Double> processPerPlugMedianTuple(Tuple tuple) {
        String key     = tuple.getField(1);
        String houseId = key.split(":")[0];
        long timestamp = tuple.getField(2);
        double value   = tuple.getField(3);

        if (globalMedianBacklog.containsKey(timestamp)) {
            OutlierTracker tracker;

            if (outliers.containsKey(houseId)) {
                tracker = outliers.get(houseId);
            } else {
                tracker = new OutlierTracker();
                outliers.put(houseId, tracker);
            }

            if (!tracker.isMember(key)) {
                tracker.addMember(key);
            }

            double globalMedian = globalMedianBacklog.get(timestamp);
            if (globalMedian < value) { // outlier
                if (!tracker.isOutlier(key)) {
                    tracker.addOutlier(key);
                    return new Tuple4<Long, Long, String, Double>(timestamp - 24 * 60 * 60, timestamp,
                            houseId, tracker.getCurrentPercentage());
                }
            } else {
                if (tracker.isOutlier(key)) {
                    tracker.removeOutlier(key);
                    //emit
                    return new Tuple4<Long, Long, String, Double>(timestamp - 24 * 60 * 60, timestamp,
                            houseId, tracker.getCurrentPercentage());
                }
            }
        } else {    // global median has not arrived
            unprocessedMessages.add(new ComparableTuple(tuple));
        }

        return null;
    }

    private class ComparableTuple implements Serializable, Comparable<ComparableTuple> {
        private final Tuple tuple;

        private ComparableTuple(Tuple tuple) {
            this.tuple = tuple;
        }

        @Override
        public int compareTo(ComparableTuple o) {
            Long field1 = this.tuple.getField(2);
            Long field2 = o.tuple.getField(2);
            return field1.compareTo(field2);
        }
    }
}