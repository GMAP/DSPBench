package flink.application.spikedetection;

import flink.constants.SpikeDetectionConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MovingAverage extends RichFlatMapFunction<Tuple3<String, Date, Double>, Tuple3<String, Double, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(MovingAverage.class);

    private static int movingAverageWindow;
    private static Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private static Map<String, Double> deviceIDtoSumOfEvents;

    Configuration config;

    Metrics metrics = new Metrics();

    public MovingAverage(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    private int window(Configuration config) {
        if (movingAverageWindow == 0) {
            movingAverageWindow = config.getInteger(SpikeDetectionConstants.Conf.MOVING_AVERAGE_WINDOW, 1000);
        }

        return movingAverageWindow;
    }

    private Map<String, LinkedList<Double>>  IdMap() {
        if (deviceIDtoStreamMap == null) {
            deviceIDtoStreamMap = new HashMap<>();
        }

        return deviceIDtoStreamMap;
    }

    private Map<String, Double>  IdSum() {
        if (deviceIDtoSumOfEvents == null) {
            deviceIDtoSumOfEvents = new HashMap<>();
        }

        return deviceIDtoSumOfEvents;
    }

    @Override
    public void flatMap(Tuple3<String, Date, Double> input, Collector<Tuple3<String, Double, Double>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        window(config);

        String deviceID = input.getField(0);
        double nextDouble = input.getField(2);
        double movingAverageInstant = movingAverage(deviceID, nextDouble);

        out.collect(new Tuple3<String, Double, Double>(deviceID, movingAverageInstant, nextDouble));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    private double movingAverage(String deviceID, double nextDouble) {
        IdMap();
        IdSum();
        LinkedList<Double> valueList = new LinkedList<>();
        double sum = 0.0;

        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow - 1) {
                double valueToRemove = valueList.removeFirst();
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum / valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }
}
