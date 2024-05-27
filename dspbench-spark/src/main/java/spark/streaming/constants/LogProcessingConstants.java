package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface LogProcessingConstants extends BaseConstants {
    String PREFIX = "lp";
    
    interface Config extends BaseConfig {
        String VOLUME_COUNTER_WINDOW  = "lp.volume_counter.window";
        String VOLUME_COUNTER_THREADS = "lp.volume_counter.threads";
        String STATUS_COUNTER_THREADS = "lp.status_counter.threads";
        String GEO_FINDER_THREADS     = "lp.geo_finder.threads";
        String GEO_STATS_THREADS      = "lp.geo_stats.threads";
    }

    interface Component {
        String SINK_VISIT = "ca.visit.sink.class";
        String SINK_LOCATION = "ca.location.sink.class";

        String VOLUME_SINK = "lp.count.sink.class";
        String STATUS_SINK = "lp.status.sink.class";
        String GEO_SINK = "lp.country.sink.class";
    }
}
