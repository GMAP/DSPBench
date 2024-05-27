package flink.application.adanalytics;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import flink.constants.AdAnalyticsConstants;
import flink.tools.NthLastModifiedTimeTracker;
import flink.tools.SlidingWindowCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Metrics;

public class RollingCTR extends Metrics implements WindowFunction<Tuple3<Long, Long, AdEvent>, Tuple6<String, String, Double, Long, Long, Integer>, Tuple2<Long, Long>, TimeWindow>{

    private static final Logger LOG = LoggerFactory.getLogger(RollingCTR.class);
    Configuration config;

    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    protected SlidingWindowCounter<String> clickCounter;
    protected SlidingWindowCounter<String> impressionCounter;
    
    protected int windowLengthInSeconds;
    protected int emitFrequencyInSeconds;

    protected NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingCTR(Configuration config) {
        this(config, 60);
    }

    public RollingCTR(Configuration config, int emitFrequencyInSeconds) {
        super.initialize(config);
        this.config = config;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;

        windowLengthInSeconds = config.getInteger(AdAnalyticsConstants.Conf.CTR_WINDOW_LENGTH, 300);
        
        int windowLenghtInSlots = windowLengthInSeconds / emitFrequencyInSeconds;

        clickCounter      = new SlidingWindowCounter<>(windowLenghtInSlots);
        impressionCounter = new SlidingWindowCounter<>(windowLenghtInSlots);
        
        lastModifiedTracker = new NthLastModifiedTimeTracker(windowLenghtInSlots);
    }

    @Override
    public void apply(Tuple2<Long, Long> key, TimeWindow window, Iterable<Tuple3<Long, Long, AdEvent>> input,
            Collector<Tuple6<String, String, Double, Long, Long, Integer>> out) throws Exception {
        super.initialize(config);

        for (Tuple3<Long, Long, AdEvent> in : input){

            super.incReceived();

            AdEvent event = (AdEvent) in.getField(2);
            String eventKey = String.format("%d:%d", event.getQueryId(), event.getAdID());
            
            if (event.getType() == AdEvent.Type.Click) {
                clickCounter.incrementCount(eventKey);
            } else if (event.getType() == AdEvent.Type.Impression) {
                impressionCounter.incrementCount(eventKey);
            }
        }

        Map<String, Long> clickCounts = clickCounter.getCountsThenAdvanceWindow();
        Map<String, Long> impressionCounts = impressionCounter.getCountsThenAdvanceWindow();
        
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }

        for (Entry<String, Long> entry : clickCounts.entrySet()) {
            String entryKey = entry.getKey();
            String[] ids = entryKey.split(":");
            
            long clicks = entry.getValue();
            long impressions = impressionCounts.get(entryKey);
            double ctr = (double)clicks / (double)impressions;

            super.incEmitted();
            
            out.collect(new Tuple6<String,String,Double,Long,Long,Integer>(ids[0], ids[1], ctr, impressions, clicks, actualWindowLengthInSeconds));
        }
    }
    
}
