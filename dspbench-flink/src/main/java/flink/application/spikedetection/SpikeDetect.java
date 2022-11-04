package flink.application.spikedetection;

import flink.application.logprocessing.GeoStats;
import flink.constants.SpikeDetectionConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class SpikeDetect implements FlatMapFunction<Tuple4<String, Double, Double, String>, Tuple5<String, Double, Double, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetect.class);

    private static double spikeThreshold;

    Configuration configs;

    public SpikeDetect(Configuration config) {
        configs = config;
        spikeThres(config);
    }

    private double  spikeThres(Configuration config) {
        if (spikeThreshold == 0) {
            spikeThreshold = config.getDouble(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THRESHOLD, 0.03d);
        }

        return spikeThreshold;
    }


    @Override
    public void flatMap(Tuple4<String, Double, Double, String> input, Collector<Tuple5<String, Double, Double, String, String>> out) {
        spikeThres(configs);
        String deviceID = input.getField(0);
        double movingAverageInstant = input.getField(1);
        double nextDouble = input.getField(2);

        if (Math.abs(nextDouble - movingAverageInstant) > spikeThreshold * movingAverageInstant) {
            out.collect(new Tuple5<String, Double, Double, String, String>(deviceID, movingAverageInstant, nextDouble, "spike detected", input.f3));
        }
        //super.calculateThroughput();
}
}
