package spark.streaming.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import java.io.File;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import spark.streaming.constants.BaseConstants.MetricReporter;
import spark.streaming.metrics.reporter.CsvReporter;
import spark.streaming.metrics.reporter.Slf4jCustomReporter;
import spark.streaming.metrics.reporter.statsd.StatsDReporter;
import spark.streaming.util.Configuration;
import static spark.streaming.util.Configuration.METRICS_INTERVAL;
import static spark.streaming.util.Configuration.METRICS_OUTPUT;
import static spark.streaming.util.Configuration.METRICS_REPORTER;
import static spark.streaming.util.Configuration.METRICS_STATSD_HOST;
import static spark.streaming.util.Configuration.METRICS_STATSD_PORT;

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
        
        String[] reporters = config.get(METRICS_REPORTER, MetricReporter.CONSOLE).split(",");
        
        for (String reporterType : reporters) {
            ScheduledReporter reporter;

            if (reporterType.equals(MetricReporter.SLF4J)) {
                reporter = Slf4jCustomReporter.forRegistry(metrics).build();
            } else if (reporterType.equals(MetricReporter.CSV)) {
                String outDir = config.get(METRICS_OUTPUT, "/tmp");
                reporter = CsvReporter.forRegistry(metrics)
                                      .formatFor(Locale.US)
                                      .convertRatesTo(TimeUnit.SECONDS)
                                      .convertDurationsTo(TimeUnit.MILLISECONDS)
                                      .build(new File(outDir));
            } else if (reporterType.equals(MetricReporter.STATSD)) {
                String host = config.get(METRICS_STATSD_HOST, "localhost");
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

            reporter.start(config.getInt(METRICS_INTERVAL, 5), TimeUnit.SECONDS);
        }

        return metrics;
    }
}
