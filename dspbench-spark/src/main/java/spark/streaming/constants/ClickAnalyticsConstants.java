package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface ClickAnalyticsConstants extends BaseConstants {
    String PREFIX = "ca";
    
    interface Config extends BaseConfig {
        String REPEATS_THREADS       = "ca.repeats.threads";
        String GEOGRAPHY_THREADS     = "ca.geography.threads";
        String TOTAL_STATS_THREADS   = "ca.total_stats.threads";
        String GEO_STATS_THREADS     = "ca.geo_stats.threads";
    }

    interface Component {
        String SINK_VISIT = "ca.visit.sink.class";
        String SINK_LOCATION = "ca.location.sink.class";
    }
}
