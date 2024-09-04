package flink.application.spikedetection;

import flink.constants.SpikeDetectionConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpikeDetect extends RichFlatMapFunction<Tuple3<String, Double, Double>, Tuple4<String, Double, Double, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetect.class);

    private static double spikeThreshold;

    Configuration config;
    Metrics metrics = new Metrics();

    public SpikeDetect(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    private double  spikeThres(Configuration config) {
        if (spikeThreshold == 0) {
            spikeThreshold = config.getDouble(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THRESHOLD, 0.03d);
        }

        return spikeThreshold;
    }

    @Override
    public void flatMap(Tuple3<String, Double, Double> input, Collector<Tuple4<String, Double, Double, String>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        spikeThres(config);
        String deviceID = input.getField(0);
        double movingAverageInstant = input.getField(1);
        double nextDouble = input.getField(2);

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            out.collect(new Tuple4<String, Double, Double, String>(deviceID, movingAverageInstant, nextDouble, "spike detected"));
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
