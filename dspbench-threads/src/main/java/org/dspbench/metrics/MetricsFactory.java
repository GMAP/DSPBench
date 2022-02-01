package org.dspbench.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import org.dspbench.core.Component;
import org.dspbench.core.Sink;
import org.dspbench.core.Source;
import org.dspbench.core.hook.Hook;
import org.dspbench.core.hook.ProcessTimeHook;
import org.dspbench.core.hook.ThroughputHook;
import org.dspbench.core.hook.TimerHook;
import org.dspbench.core.hook.TupleCounterHook;
import org.dspbench.core.hook.TupleCounterSourceHook;
import org.dspbench.core.hook.TupleLatencyHdrHook;
import org.dspbench.core.hook.TupleLatencyHook;
import org.dspbench.core.hook.TupleSizeHook;
import org.dspbench.metrics.reporter.CsvReporter;
import org.dspbench.metrics.reporter.Slf4jCustomReporter;
import org.dspbench.metrics.reporter.statsd.StatsDReporter;
import org.dspbench.utils.Configuration;
import org.dspbench.core.Constants;

import static org.dspbench.utils.Configuration.*;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mayconbordin
 */
public class MetricsFactory {
    private static Configuration config;
    
    public static MetricRegistry createRegistry(Configuration config) {
        MetricsFactory.config = config;
        
        if (!config.getBoolean(Configuration.METRICS_ENABLED, false))
            return null;
        
        MetricRegistry metrics = new MetricRegistry();
        
        //String reporterType = config.getString(METRICS_REPORTER, METRICS_REPORTER_CONSOLE);

        if (config.getBoolean(METRICS_MEMORY_ENABLED, false)) {
            MemoryUsageGaugeSet memoryUsageGaugeSet = new MemoryUsageGaugeSet();
            metrics.register("memory", memoryUsageGaugeSet);
        }

        String[] reporters = config.getString(METRICS_REPORTER, Constants.METRICS_REPORTER_CONSOLE).split(",");
        
        for (String reporterType : reporters) {
            ScheduledReporter reporter;

            if (reporterType.equals(Constants.METRICS_REPORTER_SLF4J)) {
                reporter = Slf4jCustomReporter.forRegistry(metrics).build();
            } else if (reporterType.equals(Constants.METRICS_REPORTER_CSV)) {
                String outDir = config.getString(METRICS_OUTPUT, "/tmp");
                reporter = CsvReporter.forRegistry(metrics)
                                      .formatFor(Locale.US)
                                      .convertRatesTo(TimeUnit.SECONDS)
                                      .convertDurationsTo(TimeUnit.MILLISECONDS)
                                      .build(new File(outDir));
            } else if (reporterType.equals(Constants.METRICS_REPORTER_STATSD)) {
                String host = config.getString(METRICS_STATSD_HOST, "localhost");
                int port    = config.getInt(METRICS_STATSD_PORT, 8125);

                reporter = StatsDReporter.forRegistry(metrics)
                                             .prefixedWith("streamer")
                                             .convertRatesTo(TimeUnit.SECONDS)
                                             .convertDurationsTo(TimeUnit.MILLISECONDS)
                                             .build(host, port);
            } else {
                reporter = ConsoleReporter.forRegistry(metrics)
                                          .convertRatesTo(TimeUnit.SECONDS)
                                          .convertDurationsTo(TimeUnit.MILLISECONDS)
                                          .build();
            }

            reporter.start(config.getInt(METRICS_INTERVAL), TimeUnit.SECONDS);
        }
        
        return metrics;
    }
    
    public static List<Hook> createMetricHooks(Component component, MetricRegistry metrics, String name) {
        String[] metricList = config.getString(METRICS_ENABLED_METRICS).split(",");
        List<Hook> hooks = new ArrayList<Hook>(metricList.length);
        
        for (String metric : metricList) {
            if (metric.equals(Constants.METRIC_PROCESS_TIME)) {
                hooks.add(createProcessTimeHook(metrics, name));
            } else if (metric.equals(Constants.METRIC_THROUGHPUT)) {
                hooks.add(createThroughputHook(metrics, name));
            } else if (metric.equals(Constants.METRIC_TUPLE_COUNTER)) {
                if (component instanceof Source) {
                    hooks.add(createTupleCounterSourceHook(metrics, name));
                } else {
                    hooks.add(createTupleCounterHook(metrics, name));
                }
            } else if (metric.equals(Constants.METRIC_TUPLE_SIZE)) {
                hooks.add(createTupleSizeHook(metrics, name));
            }
            
            if (component instanceof Sink) {
                if (metric.equals(Constants.METRIC_TUPLE_LATENCY)) {
                    hooks.add(createTupleLatencyHook(metrics, name));
                } else if (metric.equals(Constants.METRIC_TUPLE_LATENCY_HDR)) {
                    long hVal = config.getLong(HDR_HIGHEST_TRACKABLE_VALUE);
                    int numDig = config.getInt(HDR_NUM_SIGNIFICANT_VALUE_DIGITS);
                    hooks.add(createTupleLatencyHdrHook(metrics, name, hVal, numDig));
                }
            }
        }
        
        return hooks;
    }
    
    public static Hook createThroughputHook(MetricRegistry metrics, String name) {
        return new ThroughputHook(metrics.register(name + ".throughput", new ThroughputNew()));
    }
    
    public static Hook createTupleSizeHook(MetricRegistry metrics, String name) {
        return new TupleSizeHook(metrics.histogram(name + ".tuple-size"));
    }
    
    public static Hook createTupleCounterHook(MetricRegistry metrics, String name) {
        return new TupleCounterHook(
                metrics.counter(name + ".tuples-received"), 
                metrics.counter(name + ".tuples-emitted"));
    }
    
    public static Hook createTupleCounterSourceHook(MetricRegistry metrics, String name) {
        return new TupleCounterSourceHook(
                metrics.counter(name + ".tuples-received"), 
                metrics.counter(name + ".tuples-emitted"));
    }
    
    public static Hook createProcessTimeHook(MetricRegistry metrics, String name) {
        double sampleRate = config.getDouble(METRICS_PROCESS_TIME_SAMPLE_RATE, 0.05);
        Hook hook = new ProcessTimeHook(metrics.timer(name + ".process-time"), sampleRate);
        hook.setHighPriority();
        return hook;
    }
    
    public static Hook createTimerHook(MetricRegistry metrics, String name) {
        Hook timerHook = new TimerHook(metrics.timer(name + ".rate"));
        timerHook.setHighPriority();
        return timerHook;
    }
    
    public static Hook createTupleLatencyHook(MetricRegistry metrics, String name) {
        return new TupleLatencyHook(metrics.timer(name + ".tuple-latency"));
    }
    
    public static Hook createTupleLatencyHdrHook(MetricRegistry metrics, String name, long highestTrackableValue, int numberOfSignificantValueDigits) {
        return new TupleLatencyHdrHook(metrics.register(name + ".tuple-latency-hdr",
                new HdrHistogram(highestTrackableValue, numberOfSignificantValueDigits)));
    }
}
