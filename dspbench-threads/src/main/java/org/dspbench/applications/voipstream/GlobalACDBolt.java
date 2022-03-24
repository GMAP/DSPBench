package org.dspbench.applications.voipstream;

import org.dspbench.core.Operator;
import org.dspbench.core.Tuple;

import org.dspbench.core.Values;
import org.dspbench.utils.Configuration;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GlobalACDBolt extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACDBolt.class);

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