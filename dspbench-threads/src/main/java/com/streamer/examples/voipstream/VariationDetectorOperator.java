package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;
import com.streamer.utils.Configuration;

import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariationDetectorOperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(VariationDetectorOperator.class);
    
    private BloomFilter<String> detector;
    private int approxInsertSize;
    private double falsePostiveRate;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);
        
        approxInsertSize = config.getInt(Config.VAR_DETECT_APROX_SIZE);
        falsePostiveRate = config.getInt(Config.VAR_DETECT_ERROR_RATE);

        detector = new FilterBuilder(approxInsertSize, falsePostiveRate).buildBloomFilter();
    }

    public void process(Tuple input) {
        String callingNumber = input.getString(Field.CALLING_NUM);
        String calledNumber  = input.getString(Field.CALLED_NUM);
        boolean calledEstablished = input.getBoolean(Field.CALL_ESTABLISHED);
        long answerTime = ((DateTime) input.getValue(Field.ANSWER_TIME)).getMillis()/1000;
        int callDuration = input.getInt(Field.CALL_DURATION);
        
        String key = String.format("%s:%s", callingNumber, calledNumber);
        boolean newCallee = false;

        // check if the pair exists
        // if not, add to the detector
        if (!detector.contains(key)) {
            detector.add(key);
            newCallee = true;
        }
        
        Values values = new Values(callingNumber, calledNumber, answerTime, callDuration, newCallee, calledEstablished);
        
        emit(input, values);
        emit(Streams.VARIATIONS_BACKUP, input, values);
    }
}