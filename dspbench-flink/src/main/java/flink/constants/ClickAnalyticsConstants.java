package flink.constants;

public interface ClickAnalyticsConstants extends BaseConstants {
    String PREFIX = "ca";
    
    interface Conf extends BaseConf {
        String SOURCE_THREADS        = "ca.source.threads";
        String PARSER_THREADS        = "ca.parser.threads";
        String REPEATS_THREADS       = "ca.repeats.threads";
        String GEOGRAPHY_THREADS     = "ca.geography.threads";
        String TOTAL_STATS_THREADS   = "ca.total_stats.threads";
        String TOTAL_SINK_THREADS      = "ca.visit.sink.threads";
        String GEO_STATS_THREADS     = "ca.geo_stats.threads";
        String GEO_SINK_THREADS      = "ca.location.sink.threads";
    }
    
    interface Field {
        String IP = "ip";
        String URL = "url";
        String CLIENT_KEY = "clientKey";
        String COUNTRY = "country";
        String COUNTRY_NAME = "country_name";
        String CITY = "city";
        String UNIQUE = "unique";
        String COUNTRY_TOTAL = "countryTotal";
        String CITY_TOTAL = "cityTotal";
        String TOTAL_COUNT = "totalCount";
        String TOTAL_UNIQUE = "totalUnique";
        String INITTIME = "inittime";
    }
    
    interface Component extends BaseComponent {
        String REPEATS = "repeatsBolt";
        String GEOGRAPHY = "geographyBolt";
        String TOTAL_STATS = "totalStats";
        String GEO_STATS = "geoStats";
        String SINK_VISIT = "sinkVisit";
        String SINK_LOCATION = "sinkLocation";
    }
}
