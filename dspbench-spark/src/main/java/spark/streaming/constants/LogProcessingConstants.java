package spark.streaming.constants;

public interface LogProcessingConstants extends BaseConstants {
    String PREFIX = "lp";
    
    interface Config extends BaseConfig {
        String PARSER_THREADS         = "lp.parser.threads";
        
        String VOLUME_COUNTER_WINDOW  = "lp.volume_counter.window";
        String SINGLE_VOL_COUNTER_THREADS = "lp.single_volume_counter.threads";
        String VOLUME_COUNTER_THREADS = "lp.volume_counter.threads";
        String STATUS_COUNTER_THREADS = "lp.status_counter.threads";
        String SINGLE_STATUS_COUNTER_THREADS = "lp.single_status_counter.threads";
        String GEO_FINDER_THREADS     = "lp.geo_finder.threads";
        String COUNTRY_COUNTER_THREADS     = "lp.country_counter.threads";
        String CITY_COUNTER_THREADS     = "lp.city_counter.threads";
        String CITY_SINGLE_COUNTER_THREADS     = "lp.single_city_counter.threads";
    }
}
