package org.dspbench.applications.smartgrid;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static org.dspbench.applications.smartgrid.SmartGridConstants.*;

import org.dspbench.applications.smartgrid.SmartGridConstants.Field;
import org.dspbench.applications.smartgrid.SmartGridConstants.SlidingWindowAction;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.dspbench.util.math.RunningMedianCalculator;

/**
 * Author: Thilina
 * Date: 12/5/14
 */
public class PlugMedianCalculatorBolt extends AbstractBolt {
    private Map<String, RunningMedianCalculator> runningMedians;
    private Map<String, Long> lastUpdatedTsMap;

    @Override
    public void initialize() {
        runningMedians = new HashMap<>();
        lastUpdatedTsMap = new HashMap<>();
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        int operation = tuple.getIntegerByField(Field.SLIDING_WINDOW_ACTION);
        double value = tuple.getDoubleByField(Field.VALUE);
        long timestamp = tuple.getLongByField(Field.TIMESTAMP);
        String key = getKey(tuple);

        RunningMedianCalculator medianCalc = runningMedians.get(key);
        if (medianCalc == null) {
            medianCalc = new RunningMedianCalculator();
            runningMedians.put(key, medianCalc);
        }

        Long lastUpdatedTs = lastUpdatedTsMap.get(key);
        if (lastUpdatedTs == null) {
            lastUpdatedTs = 0l;
        }

        if (operation == SlidingWindowAction.ADD) {
            double median = medianCalc.getMedian(value);
            if (lastUpdatedTs < timestamp) {
                // the sliding window has moved
                lastUpdatedTsMap.put(key, timestamp);
                if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                    emittedThroughput();
                }
                collector.emit(new Values(key, timestamp, median));
            }
        } else {
            medianCalc.remove(value);
        }
        super.calculateThroughput();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.PLUG_SPECIFIC_KEY, Field.TIMESTAMP, Field.PER_PLUG_MEDIAN);
    }

    private String getKey(Tuple tuple) {
        return tuple.getStringByField(Field.HOUSE_ID) + ':' +
                tuple.getStringByField(Field.HOUSEHOLD_ID) + ':' +
                tuple.getStringByField(Field.PLUG_ID);
    }

}