package flink.application.smartgrid;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalMedianCalc extends RichFlatMapFunction<Tuple6<Long, String, String, String, Double, Integer>, Tuple4<String, String, Long, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalMedianCalc.class);

    private static RunningMedianCalculator medianCalc;
    private long lastUpdatedTs;

    Configuration config;
    Metrics metrics = new Metrics();

    public GlobalMedianCalc(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
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
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        createMed();
        int operation  = input.getField(5);
        double value   = input.getField(4);
        long timestamp = input.getField(0);

        if (operation == 1){
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTs = timestamp;
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple4<>("globalMedianCalculator","",timestamp, median));
            }
        } else {
            medianCalc.remove(value);
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
