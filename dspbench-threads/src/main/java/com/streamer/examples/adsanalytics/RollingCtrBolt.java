package com.streamer.examples.adsanalytics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.adsanalytics.AdsAnalyticsConstants.Config;
import com.streamer.examples.adsanalytics.AdsAnalyticsConstants.Field;
import com.streamer.examples.utils.window.NthLastModifiedTimeTracker;
import com.streamer.examples.utils.window.SlidingWindowCounter;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class RollingCtrBolt extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(RollingCtrBolt.class);
    
    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    protected SlidingWindowCounter<String> clickCounter;
    protected SlidingWindowCounter<String> impressionCounter;
    protected NthLastModifiedTimeTracker lastModifiedTracker;
    
    protected int windowLengthInSeconds;
    protected int emitFrequencyInSeconds;

    @Override
    protected void initialize() {
        windowLengthInSeconds  = config.getInt(Config.CTR_WINDOW_LENGTH, 300);
        emitFrequencyInSeconds = config.getInt(Config.CTR_EMIT_FREQUENCY, 60);
        
        int windowLenghtInSlots = windowLengthInSeconds / emitFrequencyInSeconds;

        clickCounter      = new SlidingWindowCounter<String>(windowLenghtInSlots);
        impressionCounter = new SlidingWindowCounter<String>(windowLenghtInSlots);
        
        lastModifiedTracker = new NthLastModifiedTimeTracker(windowLenghtInSlots);
    }

    @Override
    public void process(Tuple tuple) {
        countObjAndAck(tuple);
    }

    @Override
    public void onTime() {
        emitCurrentWindowCounts();
    }
    
    private void emitCurrentWindowCounts() {
        Map<String, Long> clickCounts = clickCounter.getCountsThenAdvanceWindow();
        Map<String, Long> impressionCounts = impressionCounter.getCountsThenAdvanceWindow();
        
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        
        emit(clickCounts, impressionCounts, actualWindowLengthInSeconds);
    }

    private void emit(Map<String, Long> clickCounts, Map<String, Long> impressionCounts, int actualWindowLengthInSeconds) {
        for (Entry<String, Long> entry : clickCounts.entrySet()) {
            String key = entry.getKey();
            String[] ids = key.split(":");
            
            long clicks = entry.getValue();
            long impressions = impressionCounts.get(key);
            double ctr = (double)clicks / (double)impressions;
            
            emit(new Values(ids[0], ids[1], ctr, impressions, clicks, actualWindowLengthInSeconds));
        }
    }

    protected void countObjAndAck(Tuple tuple) {
        AdEvent event = (AdEvent) tuple.getValue(Field.EVENT);
        String key = String.format("%d:%d", event.getQueryId(), event.getAdID());
        
        if (event.getType() == AdEvent.Type.Click) {
            clickCounter.incrementCount(key);
        } else if (event.getType() == AdEvent.Type.Impression) {
            impressionCounter.incrementCount(key);
        }
    }
}
