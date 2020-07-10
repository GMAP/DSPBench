package com.streamer.examples.trafficmonitoring;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.trafficmonitoring.TrafficMonitoringConstants.Config;
import com.streamer.examples.trafficmonitoring.TrafficMonitoringConstants.Field;
import com.streamer.examples.trafficmonitoring.gis.GPSRecord;
import com.streamer.examples.trafficmonitoring.gis.RoadGridList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class MapMatchingOperator extends BaseOperator {
    private static final Logger LOG = LoggerFactory.getLogger(MapMatchingOperator.class);

    private RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;

    @Override
    protected void initialize() {
        String shapeFile = config.getString(Config.MAP_MATCHER_SHAPEFILE);
        
        latMin = config.getDouble(Config.MAP_MATCHER_LAT_MIN);
        latMax = config.getDouble(Config.MAP_MATCHER_LAT_MAX);
        lonMin = config.getDouble(Config.MAP_MATCHER_LON_MIN);
        lonMax = config.getDouble(Config.MAP_MATCHER_LON_MAX);
        
        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (IOException ex) {
            throw new RuntimeException("Error while loading shape file");
        }  catch (SQLException ex) {
            throw new RuntimeException("Error while loading shape file");
        }
    }

    @Override
    public void process(Tuple input) {
        try {
            String carId  = input.getString(Field.VEHICLE_ID);
            Date date = (Date) input.getValue(Field.DATE_TIME);
            boolean occ   = input.getBoolean(Field.OCCUPIED);
            int speed        = input.getInt(Field.SPEED);
            int bearing      = input.getInt(Field.BEARING);
            double latitude  = input.getDouble(Field.LATITUDE);
            double longitude = input.getDouble(Field.LONGITUDE);
            
            if (speed <= 0) {
                return;
            }

            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin) {
                return;
            }
            
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);
            
            int roadID = sectors.fetchRoadID(record);
            
            if (roadID != -1) {
                emit(input, new Values(carId, date, occ, speed, bearing, 
                        latitude, longitude, roadID));
            }
        } catch (SQLException ex) {
            LOG.error("Unable to fetch road ID", ex);
        }
    }
    
}
