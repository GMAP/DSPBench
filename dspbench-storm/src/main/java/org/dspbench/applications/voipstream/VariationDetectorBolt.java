package org.dspbench.applications.voipstream;

import java.util.Map;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.bloom.BloomFilter;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariationDetectorBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VariationDetectorBolt.class);
    
    private BloomFilter<String> detector;
    private BloomFilter<String> learner;
    private int approxInsertSize;
    private double falsePostiveRate;
    private double cycleThreshold;

    @Override
    public Map<String, Fields> getDefaultStreamFields() {
        Map<String, Fields> streams = new HashMap<>();

        Fields fields = new Fields(VoIPSTREAMConstants.Field.CALLING_NUM, VoIPSTREAMConstants.Field.CALLED_NUM, VoIPSTREAMConstants.Field.ANSWER_TIME, VoIPSTREAMConstants.Field.NEW_CALLEE, VoIPSTREAMConstants.Field.RECORD);
        
        streams.put(VoIPSTREAMConstants.Stream.DEFAULT, fields);
        streams.put(VoIPSTREAMConstants.Stream.BACKUP, fields);
        
        return streams;
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public void initialize() {
        approxInsertSize = config.getInt(VoIPSTREAMConstants.Conf.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getDouble(VoIPSTREAMConstants.Conf.VAR_DETECT_ERROR_RATE);
        
        detector = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        learner  = new BloomFilter<>(falsePostiveRate, approxInsertSize);
        
        cycleThreshold = detector.size()/Math.sqrt(2);
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        CallDetailRecord cdr = (CallDetailRecord) input.getValueByField(VoIPSTREAMConstants.Field.RECORD);
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
        
        Values values = new Values(cdr.getCallingNumber(), cdr.getCalledNumber(), 
                cdr.getAnswerTime(), newCallee, cdr);
        
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            emittedThroughput();
            emittedThroughput();
        }
        collector.emit(values);
        collector.emit(VoIPSTREAMConstants.Stream.BACKUP, values);
    }
    
    private void rotateFilters() {
        BloomFilter<String> tmp = detector;
        detector = learner;
        learner = tmp;
        learner.clear();
    }
}