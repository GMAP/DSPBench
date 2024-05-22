package flink.application.voipstream;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import flink.util.Configurations;
import flink.util.MetricsFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.constants.VoIPStreamConstants;
import flink.constants.BaseConstants;
import flink.util.ODTDBloomFilter;

public class RCR extends RichCoFlatMapFunction<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>, Tuple5<String, String, DateTime, Boolean, CallDetailRecord>, Tuple4<String, Long, Double, CallDetailRecord>>{
    private static final Logger LOG = LoggerFactory.getLogger(ECR.class);

    Configuration config;

    protected ODTDBloomFilter filter;

    Metric metrics = new Metric();

    public RCR(Configuration config){
        metrics.initialize(config);
        this.config = config;

        int numElements       = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_NUM_ELEMENTS, "rcr"), 180000);
        int bucketsPerElement = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PEL, "rcr"), 10);
        int bucketsPerWord    = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PWR, "rcr"), 16);
        double beta           = config.getDouble(String.format(VoIPStreamConstants.Conf.FILTER_BETA, "rcr"), 0.9672);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap1(Tuple5<String, String, DateTime, Boolean, CallDetailRecord> value,
            Collector<Tuple4<String, Long, Double, CallDetailRecord>> out) throws Exception {
        // Default
        metrics.initialize(config);
        metrics.incReceived("RCR");

        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        
        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis()/1000;
            
            String callee = cdr.getCalledNumber();
            filter.add(callee, 1, timestamp);
        }
    }

    @Override
    public void flatMap2(Tuple5<String, String, DateTime, Boolean, CallDetailRecord> value,
            Collector<Tuple4<String, Long, Double, CallDetailRecord>> out) throws Exception {
        // Backup
        metrics.initialize(config);
        metrics.incReceived("RCR");

        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        
        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis()/1000;
            
            String caller = cdr.getCallingNumber();
            double rcr = filter.estimateCount(caller, timestamp);
            metrics.incEmitted("RCR");
            out.collect(new Tuple4<String,Long,Double,CallDetailRecord>(caller, timestamp, rcr, cdr));
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