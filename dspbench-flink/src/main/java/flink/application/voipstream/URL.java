package flink.application.voipstream;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import flink.constants.BaseConstants;
import flink.constants.VoIPStreamConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import flink.util.MetricsFactory;

public class URL extends RichCoFlatMapFunction<Tuple4<String, Long, Double, CallDetailRecord>, Tuple4<String, Long, Double, CallDetailRecord>, Tuple5<String, Long, Double, CallDetailRecord, String>>{
    protected static enum Source {
        ECR, RCR, ECR24, ENCR, CT24, VD, FOFIR, ACD, GACD, URL, NONE
    }

    private static final Logger LOG = LoggerFactory.getLogger(URL.class);

    Configuration config;

    Metrics metrics = new Metrics();

    protected double thresholdMin;
    protected double thresholdMax;
    protected Map<String, Entry> map;

    public URL(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;

        map = new HashMap<>();
        thresholdMin = config.getDouble(String.format(VoIPStreamConstants.Conf.SCORE_THRESHOLD_MIN, "url"), 2.0);
        thresholdMax = config.getDouble(String.format(VoIPStreamConstants.Conf.SCORE_THRESHOLD_MAX, "url"), 10.0);
    }

    @Override
    public void flatMap1(Tuple4<String, Long, Double, CallDetailRecord> value,
            Collector<Tuple5<String, Long, Double, CallDetailRecord, String>> out) throws Exception {
        // ENCR
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        CallDetailRecord cdr = (CallDetailRecord) value.getField(3);
        String number  = value.getField(0);
        long timestamp = value.getField(1);
        double rate    = value.getField(2);
        
        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId("ENCR");
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);
            
            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.ENCR)/e.get(Source.ECR));
                double score = score(thresholdMin, thresholdMax, ratio);
                
                LOG.debug(String.format("T1=%f; T2=%f; ENCR=%f; ECR=%f; Ratio=%f; Score=%f", 
                        thresholdMin, thresholdMax, e.get(Source.ENCR), e.get(Source.ECR), ratio, score));
                
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple5<String, Long, Double, CallDetailRecord, String>(number, timestamp, score, cdr, "URL"));
                map.remove(key);
            } else {
                LOG.warn(String.format("Inconsistent entry: source=%s; %s",
                        src, e.toString()));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }
    }

    @Override
    public void flatMap2(Tuple4<String, Long, Double, CallDetailRecord> value,
            Collector<Tuple5<String, Long, Double, CallDetailRecord, String>> out) throws Exception {
        // ECR
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        CallDetailRecord cdr = (CallDetailRecord) value.getField(3);
        String number  = value.getField(0);
        long timestamp = value.getField(1);
        double rate    = value.getField(2);
        
        String key = String.format("%s:%d", number, timestamp);
        Source src = parseComponentId("ECR");
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);
            e.set(src, rate);
            
            if (e.isFull()) {
                // calculate the score for the ratio
                double ratio = (e.get(Source.ENCR)/e.get(Source.ECR));
                double score = score(thresholdMin, thresholdMax, ratio);
                
                LOG.debug(String.format("T1=%f; T2=%f; ENCR=%f; ECR=%f; Ratio=%f; Score=%f", 
                        thresholdMin, thresholdMax, e.get(Source.ENCR), e.get(Source.ECR), ratio, score));
                        
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple5<String, Long, Double, CallDetailRecord, String>(number, timestamp, score, cdr, "URL"));
                map.remove(key);
            } else {
                LOG.warn(String.format("Inconsistent entry: source=%s; %s",
                        src, e.toString()));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, rate);
            map.put(key, e);
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
    
    protected Source[] getFields(){
        return new Source[]{Source.ENCR, Source.ECR};
    }

    protected static Source parseComponentId(String id) {
        if (id.equals("Variation Detector"))
            return Source.VD;
        else if (id.equals("ECR24"))
            return Source.ECR24;
        else if (id.equals("CT24"))
            return Source.CT24;
        else if (id.equals("ECR"))
            return Source.ECR;
        else if (id.equals("RCR"))
            return Source.RCR;
        else if (id.equals("ENCR"))
            return Source.ENCR;
        else if (id.equals("ACD"))
            return Source.ACD;
        else if (id.equals("GLOBAL ACD"))
            return Source.GACD;
        else if (id.equals("URL"))
            return Source.URL;
        else if (id.equals("FOFIR"))
            return Source.FOFIR;
        else
            return Source.NONE;
    }

    protected static double score(double v1, double v2, double vi) {
        double score = vi/(v1 + (v2-v1));
        if (score < 0) score = 0; 
        if (score > 1) score = 1;
        return score;
    }

    protected class Entry {
        public CallDetailRecord cdr;
        
        public Source[] fields;
        public double[] values;

        public Entry(CallDetailRecord cdr) {
            this.cdr = cdr;
            this.fields = getFields();
            
            values = new double[fields.length];
            Arrays.fill(values, Double.NaN);
        }

        public void set(Source src, double rate) {
            values[pos(src)] = rate;
        }
        
        public double get(Source src) {
            return values[pos(src)];
        }
        
        public boolean isFull() {
            for (double value : values)
                if (Double.isNaN(value))
                    return false;
            return true;
        }
        
        private int pos(Source src) {
            for (int i=0; i<fields.length; i++)
                if (fields[i] == src)
                    return i;
            return -1;
        }

        public double[] getValues() {
            return values;
        }

        @Override
        public String toString() {
            return "Entry{" + "cdr=" + cdr + ", fields=" + Arrays.toString(fields) + ", values=" + Arrays.toString(values) + '}';
        }

    }
}