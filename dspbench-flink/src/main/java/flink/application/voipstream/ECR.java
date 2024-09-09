package flink.application.voipstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.VoIPStreamConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import flink.util.ODTDBloomFilter;

public class ECR extends RichFlatMapFunction<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>, Tuple4<String, Long, Double, CallDetailRecord>>{
    private static final Logger LOG = LoggerFactory.getLogger(ECR.class);

    Configuration config;
    Metrics metrics = new Metrics();

    protected ODTDBloomFilter filter;

    public ECR(Configuration config, String configPrefix){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;

        int numElements       = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_NUM_ELEMENTS, configPrefix), 180000);
        int bucketsPerElement = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PEL, configPrefix), 10);
        int bucketsPerWord    = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PWR, configPrefix), 16);
        double beta           = config.getDouble(String.format(VoIPStreamConstants.Conf.FILTER_BETA, configPrefix), 0.9672);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple5<String, String, DateTime, Boolean, CallDetailRecord> value,
            Collector<Tuple4<String, Long, Double, CallDetailRecord>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        
        if (cdr.isCallEstablished()) {
            String caller  = cdr.getCallingNumber();
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            out.collect(new Tuple4<String,Long,Double,CallDetailRecord>(caller, timestamp, ecr, cdr));
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
