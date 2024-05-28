package flink.application.voipstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.VoIPStreamConstants;
import flink.util.Metrics;
import flink.util.ODTDBloomFilter;

public class ENCR extends Metrics implements FlatMapFunction<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>, Tuple4<String, Long, Double, CallDetailRecord>>{
    private static final Logger LOG = LoggerFactory.getLogger(ECR.class);

    Configuration config;

    protected ODTDBloomFilter filter;

    public ENCR(Configuration config){
        super.initialize(config);
        this.config = config;

        int numElements       = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_NUM_ELEMENTS, "encr"), 180000);
        int bucketsPerElement = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PEL, "encr"), 10);
        int bucketsPerWord    = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PWR, "encr"), 16);
        double beta           = config.getDouble(String.format(VoIPStreamConstants.Conf.FILTER_BETA, "encr"), 0.9672);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple5<String, String, DateTime, Boolean, CallDetailRecord> value,
            Collector<Tuple4<String, Long, Double, CallDetailRecord>> out) throws Exception {
        super.initialize(config);
        super.incReceived();

        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        boolean newCallee = value.getField(3);
        
        if (cdr.isCallEstablished() && newCallee) {
            String caller  = value.getField(0);
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double rate = filter.estimateCount(caller, timestamp);

            super.incEmitted();
            out.collect(new Tuple4<String,Long,Double,CallDetailRecord>(caller, timestamp, rate, cdr));
        }
    }
}