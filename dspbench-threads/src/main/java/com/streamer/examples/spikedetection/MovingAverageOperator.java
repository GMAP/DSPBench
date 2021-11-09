package com.streamer.examples.spikedetection;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.spikedetection.SpikeDetectionConstants.Config;
import com.streamer.examples.spikedetection.SpikeDetectionConstants.Field;
import com.streamer.utils.Configuration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Calculates the average over a window for distinct elements.
 * http://github.com/surajwaghulde/storm-example-projects
 * 
 * @author surajwaghulde
 */
public class MovingAverageOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(MovingAverageOperator.class);
    
    private int movingAverageWindow;
    private Map<String, LinkedList<Double>> deviceIDtoStreamMap;
    private Map<String, Double> deviceIDtoSumOfEvents;

    @Override
    public void initialize() {
        movingAverageWindow   = config.getInt(Config.MOVING_AVERAGE_WINDOW, 1000);
        deviceIDtoStreamMap   = new HashMap<String, LinkedList<Double>>();
        deviceIDtoSumOfEvents = new HashMap<String, Double>();
    }

    @Override
    public void process(Tuple tuple) {
        String deviceID = tuple.getString(Field.DEVICE_ID);
        double nextDouble = tuple.getDouble(Field.VALUE);
        double movingAvergeInstant = movingAverage(deviceID, nextDouble);
        
        emit(tuple, new Values(deviceID, movingAvergeInstant, nextDouble));
    }

    public double movingAverage(String deviceID, double nextDouble) {
        LinkedList<Double> valueList = new LinkedList<Double>();
        double sum = 0.0;
        
        if (deviceIDtoStreamMap.containsKey(deviceID)) {
            valueList = deviceIDtoStreamMap.get(deviceID);
            sum = deviceIDtoSumOfEvents.get(deviceID);
            if (valueList.size() > movingAverageWindow-1) {
                double valueToRemove = valueList.removeFirst();			
                sum -= valueToRemove;
            }
            valueList.addLast(nextDouble);
            sum += nextDouble;
            deviceIDtoSumOfEvents.put(deviceID, sum);
            deviceIDtoStreamMap.put(deviceID, valueList);
            return sum/valueList.size();
        } else {
            valueList.add(nextDouble);
            deviceIDtoStreamMap.put(deviceID, valueList);
            deviceIDtoSumOfEvents.put(deviceID, nextDouble);
            return nextDouble;
        }
    }
}