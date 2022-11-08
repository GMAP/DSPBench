package flink.application.spikedetection;

import flink.application.clickanalytics.GeoStats;
import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.constants.SentimentAnalysisConstants;
import flink.constants.SpikeDetectionConstants;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MovingAverage extends Metrics implements FlatMapFunction<Tuple4<String, Date, Double, String>, Tuple4<String, Double, Double, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(MovingAverage.class);

    private static int movingAverageWindow;
    private static Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private static Map<String, Double> deviceIDtoSumOfEvents;

    Configuration config;

    public MovingAverage(Configuration config) {
        super.initialize(config);
        this.config = config;
        window(config);
        IdMap();
        IdSum();
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
    public void flatMap(Tuple4<String, Date, Double, String> input, Collector<Tuple4<String, Double, Double, String>> out) {
        super.initialize(config);
        window(config);

        String deviceID = input.getField(0);
        double nextDouble = input.getField(2);
        double movingAverageInstant = movingAverage(deviceID, nextDouble);

        out.collect(new Tuple4<String, Double, Double, String>(deviceID, movingAverageInstant, nextDouble, input.f3));
        super.calculateThroughput();
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
