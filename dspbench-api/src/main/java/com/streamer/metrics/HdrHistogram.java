package com.streamer.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.Snapshot;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class HdrHistogram implements Metric {
    private static final Logger LOG = LoggerFactory.getLogger(HdrHistogram.class);
    
    private final Histogram hist;
    
    /**
     * 
     * @param highestTrackableValue The highest value to be tracked by the histogram. Must be a positive
     *                              integer that is >= 2.
     * @param numberOfSignificantValueDigits The number of significant decimal digits to which the histogram will
     *                                       maintain value resolution and separation. Must be a non-negative
     *                                       integer between 0 and 5.
     */
    public HdrHistogram(long highestTrackableValue, int numberOfSignificantValueDigits) {
        hist = new Histogram(highestTrackableValue, numberOfSignificantValueDigits);
    }
    
    public void update(long value, TimeUnit unit) {
        long valueMillis = unit.toMillis(value);
        
        try {
            hist.recordValue(valueMillis);
        } catch (ArrayIndexOutOfBoundsException ex) {
            LOG.error("Value " + valueMillis + " is outside of histogram boundaries", ex);
        }
    }

    public Snapshot getSnapshot() {
        return new HdrSnapshot(hist.copy());
    }

    public long getCount() {
        return hist.getTotalCount();
    }
    
    public static final class HdrSnapshot extends Snapshot {
        private final Histogram histogram;
        
        public HdrSnapshot(Histogram histogram) {
            this.histogram = histogram;
        }
        
        @Override
        public double getValue(double d) {
            try {
                if (d < 0 || d > 1)
                    throw new IllegalArgumentException("Quantile should be a value between 0 and 1");

                return histogram.getValueAtPercentile(d * 100.0);
            } catch (ArrayIndexOutOfBoundsException ex) {
                LOG.error("Percentile " + (d * 100.0) + " is outside histogram boundaries", ex);
            }
            
            return 0;
        }

        @Override
        public long[] getValues() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public long getMax() {
            return histogram.getMaxValue();
        }

        @Override
        public double getMean() {
            return histogram.getMean();
        }

        @Override
        public long getMin() {
            return histogram.getMinValue();
        }

        @Override
        public double getStdDev() {
            return histogram.getStdDeviation();
        }

        @Override
        public void dump(OutputStream out) {
            throw new UnsupportedOperationException("Not supported yet.");
        }
        
    }
}
