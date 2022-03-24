package Constants;

/**
 *  @author  Alessandra Fais
 *  @version June 2019
 *
 *  Constants peculiar of the TrafficMonitoring application.
 */
public interface TrafficMonitoringConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/trafficmonitoring/tm.properties";
    String DEFAULT_TOPO_NAME = "TrafficMonitoring";
    String BEIJING_SHAPEFILE = "data/beijing/roads.shp";
    String DUBLIN_SHAPEFILE = "data/dublin/roads.shp";

    interface Conf {
        String SPOUT_BEIJING = "tm.spout.beijing";
        String SPOUT_DUBLIN = "tm.spout.dublin";

        String MAP_MATCHER_SHAPEFILE = "tm.map_matcher.shapefile";
        String MAP_MATCHER_BEIJING_MIN_LAT = "tm.map_matcher.beijing.lat.min";
        String MAP_MATCHER_BEIJING_MAX_LAT = "tm.map_matcher.beijing.lat.max";
        String MAP_MATCHER_BEIJING_MIN_LON = "tm.map_matcher.beijing.lon.min";
        String MAP_MATCHER_BEIJING_MAX_LON = "tm.map_matcher.beijing.lon.max";
        String MAP_MATCHER_DUBLIN_MIN_LAT = "tm.map_matcher.dublin.lat.min";
        String MAP_MATCHER_DUBLIN_MAX_LAT = "tm.map_matcher.dublin.lat.max";
        String MAP_MATCHER_DUBLIN_MIN_LON = "tm.map_matcher.dublin.lon.min";
        String MAP_MATCHER_DUBLIN_MAX_LON = "tm.map_matcher.dublin.lon.max";
        
        String ROAD_FEATURE_ID_KEY    = "tm.road.feature.id_key";
        String ROAD_FEATURE_WIDTH_KEY = "tm.road.feature.width_key";

        String SPOUT_THREADS = "tm.spout.threads";
        String MAP_MATCHER_THREADS = "tm.map_matcher.threads";
        String SPEED_CALCULATOR_THREADS = "tm.speed_calculator.threads";
        String SINK_THREADS = "tm.sink.threads";
        String ALL_THREADS = "tm.all.threads"; // useful only with Flink
    }
    
    interface Component extends BaseComponent {
        String MAP_MATCHER = "map_matcher_bolt";
        String SPEED_CALCULATOR = "speed_calculator_bolt";
    }
    
    interface Field extends BaseField {
        String VEHICLE_ID = "vehicleID";
        String SPEED = "speed";
        String BEARING = "bearing";
        String LATITUDE = "latitude";
        String LONGITUDE = "longitude";
        String ROAD_ID = "roadID";
        String AVG_SPEED = "averageSpeed";
        String COUNT = "count";
    }

    // cities supported by the application
    interface City {
        String BEIJING = "beijing";
        String DUBLIN = "dublin";
    }

    // constants used to parse Beijing taxi traces
    interface BeijingParsing {
        int B_VEHICLE_ID_FIELD = 0; // carID
        int B_NID_FIELD = 1;
        int B_DATE_FIELD = 2;
        int B_LATITUDE_FIELD = 3;
        int B_LONGITUDE_FIELD = 4;
        int B_SPEED_FIELD = 5;
        int B_DIRECTION_FIELD = 6;
    }

    // constants used to parse Dublin bus traces
    interface DublinParsing {
        int D_TIMESTAMP_FIELD = 0;
        int D_LINE_ID_FIELD = 1;
        int D_DIRECTION_FIELD = 2;
        int D_JOURNEY_PATTERN_ID_FIELD = 3;
        int D_TIME_FRAME_FIELD = 4;
        int D_VEHICLE_JOURNEY_ID_FIELD = 5;
        int D_OPERATOR_FIELD = 6;
        int D_CONGESTION_FIELD = 7;
        int D_LONGITUDE_FIELD = 8;
        int D_LATITUDE_FIELD = 9;
        int D_DELAY_FIELD = 10;
        int D_BLOCK_ID_FIELD = 11;
        int D_VEHICLE_ID_FIELD = 12; // busID
        int D_STOP_ID_FIELD = 13;
        int D_AT_STOP_FIELD = 14;
    }
}
