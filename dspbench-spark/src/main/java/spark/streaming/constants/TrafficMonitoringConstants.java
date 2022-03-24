package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface TrafficMonitoringConstants extends BaseConstants {
    String PREFIX = "tm";
    
    interface Config extends BaseConfig {
        String MAP_MATCHER_SHAPEFILE = "tm.map_matcher.shapefile";
        String MAP_MATCHER_THREADS = "tm.map_matcher.threads";
        String MAP_MATCHER_LAT_MIN = "tm.map_matcher.lat.min";
        String MAP_MATCHER_LAT_MAX = "tm.map_matcher.lat.max";
        String MAP_MATCHER_LON_MIN = "tm.map_matcher.lon.min";
        String MAP_MATCHER_LON_MAX = "tm.map_matcher.lon.max";
        
        String SPEED_CALCULATOR_THREADS = "tm.speed_calculator.threads";
        String PARSER_THREADS           = "tm.parser.threads";
        
        String ROAD_FEATURE_ID_KEY    = "tm.road.feature.id_key";
        String ROAD_FEATURE_WIDTH_KEY = "tm.road.feature.width_key";
    }
}
