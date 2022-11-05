package flink.application.smartgrid;

import flink.application.smartgrid.window.SlidingWindow;
import flink.application.smartgrid.window.SlidingWindowCallback;
import flink.application.smartgrid.window.SlidingWindowEntry;
import flink.constants.SmartGridConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GlobalMedianCalc implements FlatMapFunction<Tuple7<Long, String, String, String, Double, Integer, String>, Tuple5<String, String, Long, Double, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalMedianCalc.class);

    private static RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;

    public GlobalMedianCalc(Configuration config) {
        createMed();
    }

    private RunningMedianCalculator createMed(){
        if (medianCalc == null) {
            medianCalc = new RunningMedianCalculator();
        }

        return medianCalc;
    }

    @Override
    public void flatMap(Tuple7<Long, String, String, String, Double, Integer, String> input, Collector<Tuple5<String, String, Long, Double, String>> out) {
        createMed();
        int operation  = input.getField(5);
        double value   = input.getField(4);
        long timestamp = input.getField(0);

        if (operation == 1){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                out.collect(new Tuple5<>("globalMedianCalculator","",timestamp, median, input.f6));
            }
        } else {
            medianCalc.remove(value);
        }
        //super.calculateThroughput();
    }
}
