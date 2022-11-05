package flink.application.trafficmonitoring;

import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.application.trafficmonitoring.gis.GPSRecord;
import flink.application.trafficmonitoring.gis.RoadGridList;
import flink.constants.SentimentAnalysisConstants;
import flink.constants.TrafficMonitoringConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.Locale;

public class MapMatching implements FlatMapFunction<Tuple8<String, DateTime, Boolean,Integer, Integer, Double, Double, String>, Tuple9<String, DateTime, Boolean,Integer, Integer, Double, Double, Integer, String>>  {

    private static final Logger LOG = LoggerFactory.getLogger(MapMatching.class);

    private transient RoadGridList sectors;
    private double latMin;
    private double latMax;
    private double lonMin;
    private double lonMax;

    Configuration configs;

    public MapMatching(Configuration config) {
        configs = config;
        loadShapefile(config);
    }

    private void loadShapefile(Configuration config) {
        String shapeFile = config.getString(TrafficMonitoringConstants.Conf.MAP_MATCHER_SHAPEFILE,"/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-flink/data/beijing/roads.shp");

        latMin = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LAT_MIN,39.689602);
        latMax = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LAT_MAX,40.122410);
        lonMin = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LON_MIN,116.105789);
        lonMax = config.getDouble(TrafficMonitoringConstants.Conf.MAP_MATCHER_LON_MAX,116.670021);

        try {
            sectors = new RoadGridList(config, shapeFile);
        } catch (SQLException | IOException ex) {
            LOG.error("Error while loading shape file", ex);
            throw new RuntimeException("Error while loading shape file");
        }
    }

    private RoadGridList getSectors() {
        if (sectors == null) {
            loadShapefile(configs);
        }

        return sectors;
    }

    @Override
    public void flatMap(Tuple8<String, DateTime, Boolean,Integer, Integer, Double, Double, String> input, Collector<Tuple9<String, DateTime, Boolean,Integer, Integer, Double, Double, Integer, String>> out){
        RoadGridList gridList = getSectors();
        try {
            int speed = input.getField(3);
            int bearing = input.getField(4);
            double latitude = input.getField(5);
            double longitude = input.getField(6);

            if (speed <= 0) return;
            if (longitude > lonMax || longitude < lonMin || latitude > latMax || latitude < latMin) return;

            GPSRecord record = new GPSRecord(longitude, latitude, speed, bearing);

            int roadID = gridList.fetchRoadID(record);

            if (roadID != -1) {
                out.collect(new Tuple9<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer, String>(input.f0, input.f1, input.f2, input.f3, input.f4, input.f5, input.f6, roadID,input.f7));
            }
        } catch (SQLException ex) {
            System.out.println("Unable to fetch road ID " + ex);
        }
    }
}
