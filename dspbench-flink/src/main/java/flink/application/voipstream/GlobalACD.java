package flink.application.voipstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.VoIPStreamConstants;
import flink.util.Metrics;
import flink.util.VariableEWMA;

public class GlobalACD extends Metrics implements FlatMapFunction<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>, Tuple2<Long, Double>>{
    private static final Logger LOG = LoggerFactory.getLogger(GlobalACD.class);
    
    private VariableEWMA avgCallDuration;
    private double decayFactor;

    Configuration config;

    public GlobalACD(Configuration config){
        super.initialize(config);
        this.config = config;

        decayFactor = config.getDouble(VoIPStreamConstants.Conf.ACD_DECAY_FACTOR, 86400); //86400s = 24h
        avgCallDuration = new VariableEWMA(decayFactor);
    }

    @Override
    public void flatMap(Tuple5<String, String, DateTime, Boolean, CallDetailRecord> value,
            Collector<Tuple2<Long, Double>> out) throws Exception {
        super.initialize(config);
        super.incBoth();
        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        long timestamp = cdr.getAnswerTime().getMillis()/1000;

        avgCallDuration.add(cdr.getCallDuration());
        out.collect(new Tuple2<Long,Double>(timestamp, avgCallDuration.getAverage()));
    }
    
}