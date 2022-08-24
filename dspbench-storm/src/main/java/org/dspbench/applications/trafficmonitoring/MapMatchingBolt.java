package org.dspbench.applications.trafficmonitoring;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.applications.trafficmonitoring.gis.GPSRecord;
import org.dspbench.applications.trafficmonitoring.gis.RoadGridList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn 
 */
public class MapMatchingBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MapMatchingBolt.class);

    private RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;

    @Override
    public void initialize() {
        String shapeFile = config.getString(TrafficMonitoringConstants.Conf.MAP_MATCHER_SHAPEFILE);
        
        latMin = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LAT_MIN);
        latMax = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LAT_MAX);
        lonMin = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LON_MIN);
        lonMax = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LON_MAX);
        
        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
    }

    @Override
    public void execute(Tuple input) {
        try {
            int speed        = input.getIntegerByField(TrafficMonitoringConstants.Field.SPEED);
            int bearing      = input.getIntegerByField(TrafficMonitoringConstants.Field.BEARING);
            double latitude  = input.getDoubleByField(TrafficMonitoringConstants.Field.LATITUDE);
            double longitude = input.getDoubleByField(TrafficMonitoringConstants.Field.LONGITUDE);
            
            if (speed <= 0) return;
            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin) return;
            
            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);
            
            int roadID = sectors.fetchRoadID(record);
            
            if (roadID != -1) {
                List<Object> values = new ArrayList<>(input.getValues());
                values.add(roadID);
                collector.emit(input, values);
            }
            
            collector.ack(input);
        } catch (SQLException ex) {
            LOG.error("Unable to fetch road ID", ex);
        }
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(TrafficMonitoringConstants.Field.VEHICLE_ID, TrafficMonitoringConstants.Field.DATE_TIME, TrafficMonitoringConstants.Field.OCCUPIED, TrafficMonitoringConstants.Field.SPEED,
                TrafficMonitoringConstants.Field.BEARING, TrafficMonitoringConstants.Field.LATITUDE, TrafficMonitoringConstants.Field.LONGITUDE, TrafficMonitoringConstants.Field.INITTIME, TrafficMonitoringConstants.Field.ROAD_ID);
    }
}
