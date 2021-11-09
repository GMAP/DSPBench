package com.streamer.metrics.reporter;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
public class BatchCsvReporter extends ScheduledReporter {
    /**
     * Returns a new {@link Builder} for {@link CsvReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link CsvReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link CsvReporter} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formatFor(Locale locale) {
            this.locale = locale;
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
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
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
         * Builds a {@link CsvReporter} with the given properties, writing {@code .csv} files to the
         * given directory.
         *
         * @param directory the directory in which the {@code .csv} files will be created
         * @return a {@link CsvReporter}
         */
        public BatchCsvReporter build(File output) {
            return new BatchCsvReporter(registry,
                                   output,
                                   locale,
                                   rateUnit,
                                   durationUnit,
                                   clock,
                                   filter);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCsvReporter.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final Locale locale;
    private final Clock clock;
    private final File output;
    private final BufferedWriter writer;
    private final List<String> records;
    private final JSONParser parser;

    private BatchCsvReporter(MetricRegistry registry,
                        File output,
                        Locale locale,
                        TimeUnit rateUnit,
                        TimeUnit durationUnit,
                        Clock clock,
                        MetricFilter filter) {
        super(registry, "csv-reporter", filter, rateUnit, durationUnit);
        this.output = output;
        this.locale = locale;
        this.clock = clock;
        
        records = new ArrayList<String>();
        parser = new JSONParser();
        
        try {
            writer = new BufferedWriter(new FileWriter(output));
        } catch (IOException ex) {
            throw new RuntimeException("Output file does not exists: "+output.getAbsolutePath(), ex);
        }
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
        
        flush();
    }

    private void reportTimer(long timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        /*report(timestamp,
               name,
               "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
               "%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,calls/%s,%s",
               timer.getCount(),
               convertDuration(snapshot.getMax()),
               convertDuration(snapshot.getMean()),
               convertDuration(snapshot.getMin()),
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
               getDurationUnit());*/
    }

    private void reportMeter(long timestamp, String name, Meter meter) {
        
        JSONObject values = new JSONObject();
        values.put("timestamp", timestamp);
        values.put("name", name);
        values.put("count", meter.getCount());
        values.put("mean_rate", convertRate(meter.getMeanRate()));
        values.put("m1_rate", convertRate(meter.getOneMinuteRate()));
        

        records.add(values.toJSONString());
    }

    private void reportHistogram(long timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

        String values = "";
        for (long v : snapshot.getValues())
            values += v + "|";
        
        /*report(timestamp,
               name,
               "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,values",
               "%d,%d,%f,%d,%f,%f,%f,%f,%f,%f,%f,%s",
               histogram.getCount(),
               snapshot.getMax(),
               snapshot.getMean(),
               snapshot.getMin(),
               snapshot.getStdDev(),
               snapshot.getMedian(),
               snapshot.get75thPercentile(),
               snapshot.get95thPercentile(),
               snapshot.get98thPercentile(),
               snapshot.get99thPercentile(),
               snapshot.get999thPercentile(),
               values);*/
    }

    private void reportCounter(long timestamp, String name, Counter counter) {
        //report(timestamp, name, "count", "%d", counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge gauge) {
        //report(timestamp, name, "value", "%s", gauge.getValue());
    }
    
    private void flush() {
        try {
            for (String record : records) {
                writer.write(record);
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            LOGGER.warn("Error writing to {}", output.getName(), e);
        }
    }

    protected String sanitize(String name) {
        return name;
    }
}