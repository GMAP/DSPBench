package com.streamer.examples.voipstream;

import com.streamer.core.Operator;
import com.streamer.core.Tuple;

import com.streamer.core.Values;
import com.streamer.utils.Configuration;
import org.apache.log4j.Logger;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends Operator {
    private static final Logger LOG = Logger.getLogger(GlobalACDBolt.class);

    private VariableEWMA avgCallDuration;
    private double age;

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);

        age = config.getDouble(Config.ACD_DECAY_FACTOR); //86400s = 24h
        avgCallDuration = new VariableEWMA(age);
    }

    public void process(Tuple input) {
        long timestamp = input.getLong(Field.ANSWER_TIME);
        int callDuration = input.getInt(Field.CALL_DURATION);

        avgCallDuration.add(callDuration);

        emit(new Values(timestamp, avgCallDuration.getAverage()));
    }
}