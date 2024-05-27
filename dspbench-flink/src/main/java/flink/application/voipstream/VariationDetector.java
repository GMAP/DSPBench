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
import flink.util.BloomFilter;
import flink.util.Metrics;

public class VariationDetector extends Metrics implements FlatMapFunction<Tuple4<String, String, DateTime, CallDetailRecord>, Tuple5<String, String, DateTime, Boolean, CallDetailRecord>>{

    private static final Logger LOG = LoggerFactory.getLogger(VariationDetector.class);
    Configuration config;

    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;

    public VariationDetector(Configuration config){
        super.initialize(config);
        this.config = config;

        approxInsertSize = config.getInteger(VoIPStreamConstants.Conf.VAR_DETECT_APROX_SIZE, 180000);
        falsePostiveRate = config.getDouble(VoIPStreamConstants.Conf.VAR_DETECT_ERROR_RATE, 0.01);
        
        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner  = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        
        cycleThreshold = detector.size()/Math.sqrt(2);
    }

    @Override
    public void flatMap(Tuple4<String, String, DateTime, CallDetailRecord> value,
            Collector<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>> out) throws Exception {
        super.initialize(config);
        super.incBoth();
        CallDetailRecord cdr = (CallDetailRecord) value.getField(3);
        String key = String.format("%s:%s", cdr.getCallingNumber(), cdr.getCalledNumber());
        boolean newCallee = false;
        
        // add pair to learner
        learner.add(key);
        
        // check if the pair exists
        // if not, add to the detector
        if (!detector.membershipTest(key)) {
            detector.add(key);
            newCallee = true;
        }
        
        // if number of non-zero bits is above threshold, rotate filters
        if (detector.getNumNonZero() > cycleThreshold) {
            rotateFilters();
        }
        
        out.collect(new Tuple5<String,String,DateTime,Boolean,CallDetailRecord>(cdr.getCallingNumber(), cdr.getCalledNumber(), cdr.getAnswerTime(), newCallee, cdr));
    }
    
    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}
