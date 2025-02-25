package org.dspbench.metrics.reporter;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.dspbench.metrics.Throughput;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

/**
 * A reporter class for logging metrics values to a SLF4J {@link Logger} periodically, similar to
 * {@link ConsoleReporter} or {@link CsvReporter}, but using the SLF4J framework instead. It also
 * supports specifying a {@link Marker} instance that can be used by custom appenders and filters
 * for the bound logging toolkit to further process metrics reports.
 */
public class Slf4jCustomReporter extends ScheduledReporter {
    /**
     * Returns a new {@link Builder} for {@link Slf4jCustomReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link Slf4jCustomReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link CsvReporter} instances. Defaults to logging to {@code metrics}, not
     * using a marker, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Logger logger;
        private Marker marker;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.logger = LoggerFactory.getLogger("metrics");
            this.marker = null;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
        }

        /**
         * Log metrics to the given logger.
         *
         * @param logger an SLF4J {@link Logger}
         * @return {@code this}
         */
        public Builder outputTo(Logger logger) {
            this.logger = logger;
            return this;
        }

        /**
         * Mark all logged metrics with the given marker.
         *
         * @param marker an SLF4J {@link Marker}
         * @return {@code this}
         */
        public Builder markWith(Marker marker) {
            this.marker = marker;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Builds a {@link Slf4jCustomReporter} with the given properties.
         *
         * @return a {@link Slf4jCustomReporter}
         */
        public Slf4jCustomReporter build() {
            return new Slf4jCustomReporter(registry, logger, marker, rateUnit, durationUnit, filter);
        }
    }

    private final Logger logger;
    private final Marker marker;

    private Slf4jCustomReporter(MetricRegistry registry,
                          Logger logger,
                          Marker marker,
                          TimeUnit rateUnit,
                          TimeUnit durationUnit,
                          MetricFilter filter) {
        super(registry, "logger-reporter", filter, rateUnit, durationUnit);
        this.logger = logger;
        this.marker = marker;
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        for (Entry<String, Gauge> entry : gauges.entrySet()) {
            logGauge(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Counter> entry : counters.entrySet()) {
            logCounter(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Histogram> entry : histograms.entrySet()) {
            logHistogram(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Meter> entry : meters.entrySet()) {
            logMeter(entry.getKey(), entry.getValue());
        }

        for (Entry<String, Timer> entry : timers.entrySet()) {
            logTimer(entry.getKey(), entry.getValue());
        }
    }

    private void logTimer(String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();
        logger.info(marker,
                    "type=TIMER, name={}, count={}, min={}, max={}, mean={}, stddev={}, median={}, " +
                            "p75={}, p95={}, p98={}, p99={}, p999={}, mean_rate={}, m1={}, m5={}, " +
                            "m15={}, rate_unit={}, duration_unit={}",
                    name,
                    timer.getCount(),
                    convertDuration(snapshot.getMin()),
                    convertDuration(snapshot.getMax()),
                    convertDuration(snapshot.getMean()),
                    convertDuration(snapshot.getStdDev()),
                    convertDuration(snapshot.getMedian()),
                    convertDuration(snapshot.get75thPercentile()),
                    convertDuration(snapshot.get95thPercentile()),
                    convertDuration(snapshot.get98thPercentile()),
                    convertDuration(snapshot.get99thPercentile()),
                    convertDuration(snapshot.get999thPercentile()),
                    convertRate(timer.getMeanRate()),
                    convertRate(timer.getOneMinuteRate()),
                    convertRate(timer.getFiveMinuteRate()),
                    convertRate(timer.getFifteenMinuteRate()),
                    getRateUnit(),
                    getDurationUnit());
    }

    private void logMeter(String name, Meter meter) {
        logger.info(marker,
                    "type=METER, name={}, count={}, mean_rate={}, m1={}, m5={}, m15={}, rate_unit={}",
                    name,
                    meter.getCount(),
                    convertRate(meter.getMeanRate()),
                    convertRate(meter.getOneMinuteRate()),
                    convertRate(meter.getFiveMinuteRate()),
                    convertRate(meter.getFifteenMinuteRate()),
                    getRateUnit());
    }

    private void logHistogram(String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();
        logger.info(marker,
                    "type=HISTOGRAM, name={}, count={}, min={}, max={}, mean={}, stddev={}, " +
                            "median={}, p75={}, p95={}, p98={}, p99={}, p999={}",
                    name,
                    histogram.getCount(),
                    snapshot.getMin(),
                    snapshot.getMax(),
                    snapshot.getMean(),
                    snapshot.getStdDev(),
                    snapshot.getMedian(),
                    snapshot.get75thPercentile(),
                    snapshot.get95thPercentile(),
                    snapshot.get98thPercentile(),
                    snapshot.get99thPercentile(),
                    snapshot.get999thPercentile());
    }

    private void logCounter(String name, Counter counter) {
        logger.info(marker, "type=COUNTER, name={}, count={}", name, counter.getCount());
    }

    private void logGauge(String name, Gauge gauge) {
        if (gauge instanceof Throughput) {
            logger.info(marker, "{} throughput: {}", name, gauge.getValue());
        } else {
            logger.info(marker, "type=GAUGE, name={}, value={}", name, gauge.getValue());
        }
    }

    @Override
    protected String getRateUnit() {
        return "events/" + super.getRateUnit();
    }
}