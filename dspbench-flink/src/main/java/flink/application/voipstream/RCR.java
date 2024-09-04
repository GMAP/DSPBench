package flink.application.voipstream;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import flink.util.Configurations;
import flink.util.Metrics;
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

    Metrics metrics = new Metrics();

    public RCR(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
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
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

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
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        
        if (cdr.isCallEstablished()) {
            long timestamp = cdr.getAnswerTime().getMillis()/1000;
            
            String caller = cdr.getCallingNumber();
            double rcr = filter.estimateCount(caller, timestamp);
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            out.collect(new Tuple4<String,Long,Double,CallDetailRecord>(caller, timestamp, rcr, cdr));
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}