package flink.application.voipstream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import flink.util.Configurations;
import flink.util.Metrics;
import flink.constants.VoIPStreamConstants;

public class Scorer extends RichFlatMapFunction<Tuple5<String, Long, Double, CallDetailRecord, String>, Tuple4<String, Long, Double, CallDetailRecord>>{
    protected static enum Source {
        ECR, RCR, ECR24, ENCR, CT24, VD, FOFIR, ACD, GACD, URL, NONE
    }

    private static final Logger LOG = LoggerFactory.getLogger(Scorer.class);

    Configuration config;
    Metrics metrics = new Metrics();

    protected Map<String, Entry> map;
    private double[] weights;

    public Scorer(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;

        map = new HashMap<>();
        // parameters
        double fofirWeight = config.getDouble(VoIPStreamConstants.Conf.FOFIR_WEIGHT, 2.0);
        double urlWeight   = config.getDouble(VoIPStreamConstants.Conf.URL_WEIGHT, 3.0);
        double acdWeight   = config.getDouble(VoIPStreamConstants.Conf.ACD_WEIGHT, 3.0);
        
        weights = new double[3];
        weights[0] = fofirWeight;
        weights[1] = urlWeight;
        weights[2] = acdWeight;
    }

    @Override
    public void flatMap(Tuple5<String, Long, Double, CallDetailRecord, String> value,
            Collector<Tuple4<String, Long, Double, CallDetailRecord>> out) throws Exception {
        
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        CallDetailRecord cdr = (CallDetailRecord) value.getField(3);
        Source src     = parseComponentId(value.getField(4));
        String caller  = cdr.getCallingNumber();
        long timestamp = cdr.getAnswerTime().getMillis()/1000;
        double score   = value.getField(2);
        String key     = String.format("%s:%d", caller, timestamp);
        LOG.info(src + " - " + key);
        
        if (map.containsKey(key)) {
            Entry e = map.get(key);

            e.set(src, score);
            
            if (e.isFull()) {
                double mainScore = sum(e.getValues(), weights);
                
                LOG.debug(String.format("Score=%f; Scores=%s", mainScore, Arrays.toString(e.getValues())));
                
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple4<String,Long,Double,CallDetailRecord>(caller, timestamp, mainScore, cdr));
            }
        } else {
            Entry e = new Entry(cdr);
            e.set(src, score);
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
    
    private static double sum(double[] data, double[] weights) {
        double sum = 0.0;

        for (int i=0; i<data.length; i++) {
            sum += (data[i] * weights[i]);
        }
        
        return sum;
    }

    protected Source[] getFields(){
        return new Source[]{Source.FOFIR, Source.URL, Source.ACD};
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