package flink.application.smartgrid;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalMedianCalc extends Metrics implements FlatMapFunction<Tuple6<Long, String, String, String, Double, Integer>, Tuple4<String, String, Long, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalMedianCalc.class);

    private static RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;

    Configuration config;

    public GlobalMedianCalc(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    private RunningMedianCalculator createMed(){
        if (medianCalc == null) {
            medianCalc = new RunningMedianCalculator();
        }

        return medianCalc;
    }

    @Override
    public void flatMap(Tuple6<Long, String, String, String, Double, Integer> input, Collector<Tuple4<String, String, Long, Double>> out) {
        super.initialize(config);
        super.incReceived();
        createMed();
        int operation  = input.getField(5);
        double value   = input.getField(4);
        long timestamp = input.getField(0);

        if (operation == 1){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                super.incEmitted();
                out.collect(new Tuple4<>("globalMedianCalculator","",timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
    }
}
