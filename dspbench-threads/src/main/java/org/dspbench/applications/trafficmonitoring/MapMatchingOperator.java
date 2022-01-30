package org.dspbench.applications.trafficmonitoring;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.trafficmonitoring.TrafficMonitoringConstants.Config;
import org.dspbench.applications.trafficmonitoring.TrafficMonitoringConstants.Field;
import org.dspbench.applications.trafficmonitoring.gis.GPSRecord;
import org.dspbench.applications.trafficmonitoring.gis.RoadGridList;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

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
