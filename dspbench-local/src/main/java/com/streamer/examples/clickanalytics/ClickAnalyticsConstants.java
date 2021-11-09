package com.streamer.examples.clickanalytics;

import com.streamer.base.constants.BaseConstants;

public interface ClickAnalyticsConstants extends BaseConstants {
    String PREFIX = "ca";
    
    interface Config extends BaseConfig {
        String REPEATS_THREADS       = "ca.repeats.threads";
        String GEOGRAPHY_THREADS     = "ca.geography.threads";
        String GEO_STATS_THREADS     = "ca.geo_stats.threads";
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
    }
    
    interface Component extends BaseComponent {
        String REPEATS = "repeatsBolt";
        String GEOGRAPHY = "geographyBolt";
        String TOTAL_STATS = "totalStats";
        String GEO_STATS = "geoStats";
        String SINK_VISIT = "sinkVisit";
        String SINK_LOCATION = "sinkLocation";
    }
    
    interface Streams {
        String CLICKS          = "clickStream";
        String VISITS          = "visitStream";
        String LOCATIONS       = "locationStream";
        String TOTAL_VISITS    = "totalVisitStream";
        String TOTAL_LOCATIONS = "totalLocationStream";
    }
}
