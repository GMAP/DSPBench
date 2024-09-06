package org.dspbench.metrics;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.*;
import org.dspbench.util.config.Configuration;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class MetricsFactory {
    public static final String CONSOLE_REPORTER = "console";
    public static final String CSV_REPORTER     = "csv";
    public static final String SLF4J_REPORTER   = "slf4j";
    
    public static MetricRegistry createRegistry(Configuration config) {
        if (!config.getBoolean(Configuration.METRICS_ENABLED, false))
            return null;
        
        MetricRegistry registry = new MetricRegistry();
        
        String reporterType = "csv";//config.getString(Configuration.METRICS_REPORTER, CONSOLE_REPORTER);
        ScheduledReporter reporter;

        switch (reporterType) {
            case SLF4J_REPORTER:
                reporter = Slf4jReporter.forRegistry(registry)
                        .outputTo(LoggerFactory.getLogger("storm.applications.metrics"))
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();
                break;
            case CSV_REPORTER:
                String outDir = config.getString(Configuration.METRICS_OUTPUT, "/tmp");
                reporter = CsvReporter.forRegistry(registry)
                        .formatFor(Locale.US)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build(new File(outDir));
                break;
            default:
                reporter = ConsoleReporter.forRegistry(registry)
                        .convertRatesTo(TimeUnit.SECONDS)
                        .convertDurationsTo(TimeUnit.MILLISECONDS)
                        .build();
                break;
        }
        
        int interval = 1;//config.getInt(Configuration.METRICS_INTERVAL_VALUE, 5);
        TimeUnit unit = TimeUnit.valueOf(config.getString(Configuration.METRICS_INTERVAL_UNIT, "SECONDS").toUpperCase());
        
        reporter.start(interval, unit);
        return registry;
    }
}
