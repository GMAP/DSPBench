package flink.application.smartgrid;

import flink.application.trafficmonitoring.collections.FixedMap;
import flink.constants.BaseConstants;
import flink.constants.SmartGridConstants;
import flink.util.Configurations;
import flink.util.MetricsFactory;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class OutlierDetect extends RichCoFlatMapFunction<Tuple4<String, String, Long, Double>,Tuple4<String, String, Long, Double>, Tuple4<Long, Long, String, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(OutlierDetect.class);

    private static FixedMap<Long, Double> globalMedianBacklog;
    private static Map<String, OutlierTracker> outliers;
    private static PriorityQueue<ComparableTuple> unprocessedMessages;

    Metric metrics = new Metric();

    Configuration config;

    public OutlierDetect(Configuration config) {
        metrics.initialize(config);
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
        metrics.initialize(config);
        med();
        outl();
        msg();

        metrics.incReceived(this.getClass().getSimpleName());

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
                    metrics.incEmitted(this.getClass().getSimpleName());
                    out.collect(new Tuple4<>(dados.f0, dados.f1, dados.f2, dados.f3));
                }
            }
        } else {
            processPerPlugMedianTuple(input);
        }
    }

    @Override
    public void flatMap2(Tuple4<String, String, Long, Double> input, Collector<Tuple4<Long, Long, String, Double>> out) throws Exception {
        metrics.initialize(config);
        med();
        outl();
        msg();

        metrics.incReceived(this.getClass().getSimpleName());

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
                    metrics.incEmitted(this.getClass().getSimpleName());
                    out.collect(new Tuple4<>(dados.f0, dados.f1, dados.f2, dados.f3));
                }
            }
        } else {
            processPerPlugMedianTuple(input);
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

class Metric implements Serializable {
    Configuration config;
    private final Map<String, Long> throughput = new HashMap<>();
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(150);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(Metric.class);

    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;

    public void initialize(Configuration config) {
        this.config = config;
        getMetrics();
        File pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK")).toFile();

        pathTrh.mkdirs();

        this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
    }

    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    System.out.println("Error while writing the file " + file + " - " + ex);
                }
            } catch (Exception e) {
                System.out.println("Error while creating the file " + e.getMessage());
            }
        }).start();
    }

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived(String name) {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(name + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted(String name) {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(name + "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived(String name) {
        getTuplesReceived(name).inc();
    }

    protected void incReceived(String name, long n) {
        getTuplesReceived(name).inc(n);
    }

    protected void incEmitted(String name) {
        getTuplesEmitted(name).inc();
    }

    protected void incEmitted(String name, long n) {
        getTuplesEmitted(name).inc(n);
    }

    protected void incBoth(String name) {
        getTuplesReceived(name).inc();
        getTuplesEmitted(name).inc();
    }
}
