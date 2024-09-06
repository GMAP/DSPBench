package org.dspbench.applications.spikedetection;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.applications.spikedetection.SpikeDetectionConstants.Conf;
import org.dspbench.applications.spikedetection.SpikeDetectionConstants.Field;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 *
 * @author surajwaghulde
 */
public class MovingAverageBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageBolt.class);

    private int movingAverageWindow;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<String, Double> deviceIDtoSumOfEvents;

    @Override
    public void initialize() {
        movingAverageWindow = config.getInt(Conf.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap = new HashMap<>();
        deviceIDtoSumOfEvents = new HashMap<>();
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            recemitThroughput();
        }
        String deviceID = input.getStringByField(Field.DEVICE_ID);
        double nextDouble = input.getDoubleByField(Field.VALUE);
        double movingAvergeInstant = movingAverage(deviceID, nextDouble);

        collector.emit(input, new Values(deviceID, movingAvergeInstant, nextDouble));
        collector.ack(input);
    }

    public double movingAverage(String deviceID, double nextDouble) {
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

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.DEVICE_ID, Field.MOVING_AVG, Field.VALUE);
    }
}