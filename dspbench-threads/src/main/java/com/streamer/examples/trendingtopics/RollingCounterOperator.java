package com.streamer.examples.trendingtopics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.trendingtopics.TrendingTopicsConstants.Config;
import com.streamer.examples.trendingtopics.TrendingTopicsConstants.Field;
import com.streamer.examples.utils.window.NthLastModifiedTimeTracker;
import com.streamer.examples.utils.window.SlidingWindowCounter;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 */
public class RollingCounterOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(RollingCounterOperator.class);

    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    private SlidingWindowCounter<String> counter;
    private NthLastModifiedTimeTracker lastModifiedTracker;
    private int windowLengthInSeconds;
    private int emitFrequencyInSeconds;
    private Tuple firstParent;

    @Override
    public void initialize() {
        windowLengthInSeconds  = config.getInt(Config.COUNTER_WINDOW, 300);
        emitFrequencyInSeconds = config.getInt(Config.COUNTER_FREQ, 60);
        
        int numChunks = windowLengthInSeconds/emitFrequencyInSeconds;
        
        counter = new SlidingWindowCounter<String>(numChunks);
        lastModifiedTracker = new NthLastModifiedTimeTracker(numChunks);
    }

    @Override
    public void onTime() {
        LOG.debug("Received tick tuple, triggering emit of current window counts");
        
        Map<String, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }
        
        emit(counts, firstParent, actualWindowLengthInSeconds);
        firstParent = null;
    } 

    @Override
    public void process(Tuple tuple) {
        String obj = tuple.getString(Field.WORD);
        counter.incrementCount(obj);
        
        if (firstParent == null) {
            firstParent = tuple;
        }
    }

    private void emit(Map<String, Long> counts, Tuple parent, int actualWindowLengthInSeconds) {
        for (Entry<String, Long> entry : counts.entrySet()) {
            String obj = entry.getKey();
            Long count = entry.getValue();
            emit(parent, new Values(obj, count, actualWindowLengthInSeconds));
        }
    }
}