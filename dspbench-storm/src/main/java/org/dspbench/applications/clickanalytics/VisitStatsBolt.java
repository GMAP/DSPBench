package org.dspbench.applications.clickanalytics;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.dspbench.applications.clickanalytics.ClickAnalyticsConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;

/**
 * User: domenicosolazzo
 */
public class VisitStatsBolt extends AbstractBolt {
    private int total = 0;
    private int uniqueCount = 0;

    @Override
    public void initialize() {
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
        boolean unique = Boolean.parseBoolean(input.getStringByField(ClickAnalyticsConstants.Field.UNIQUE));
        total++;
        if(unique) uniqueCount++;
        
        collector.emit(input, new Values(total, uniqueCount));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(ClickAnalyticsConstants.Field.TOTAL_COUNT, ClickAnalyticsConstants.Field.TOTAL_UNIQUE);
    }
}
