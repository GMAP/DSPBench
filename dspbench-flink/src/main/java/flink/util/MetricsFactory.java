package flink.util;

import com.codahale.metrics.*;
import org.apache.flink.configuration.Configuration;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author mayconbordin
 */
public class MetricsFactory {
    public static final String CSV_REPORTER     = "csv";
    public static final String SLF4J_REPORTER   = "slf4j";
    
    public static MetricRegistry createRegistry(Configuration config) {
        if (!Boolean.parseBoolean(config.getString(Configurations.METRICS_ENABLED, "false"))) {
            return null;
        }

        MetricRegistry registry = new MetricRegistry();

        String reporterType = "csv";//config.getString(Configurations.METRICS_REPORTER, "csv");
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
                String outDir = config.getString(Configurations.METRICS_OUTPUT, "/home/metrics/IDK/");
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
        
        int interval = 1;//Integer.parseInt(config.getString(Configurations.METRICS_INTERVAL_VALUE, "5"));
        TimeUnit unit = TimeUnit.valueOf(config.getString(Configurations.METRICS_INTERVAL_UNIT, "SECONDS").toUpperCase());
        
        reporter.start(interval, unit);
        return registry;
    }
}
