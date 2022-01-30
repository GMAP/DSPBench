package org.dspbench.applications.trafficmonitoring;

import org.dspbench.base.task.BasicTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class TrafficMonitoringTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoringTask.class);
    
    private int mapMatcherThreads;
    private int speedCalcThreads;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        mapMatcherThreads = config.getInt(TrafficMonitoringConstants.Config.MAP_MATCHER_THREADS, 1);
        speedCalcThreads  = config.getInt(TrafficMonitoringConstants.Config.SPEED_CALCULATOR_THREADS, 1);
    }
    
    public void initialize() {
        Schema carSchema = new Schema(TrafficMonitoringConstants.Field.VEHICLE_ID, TrafficMonitoringConstants.Field.DATE_TIME, TrafficMonitoringConstants.Field.OCCUPIED,
                TrafficMonitoringConstants.Field.SPEED, TrafficMonitoringConstants.Field.BEARING, TrafficMonitoringConstants.Field.LATITUDE, TrafficMonitoringConstants.Field.LONGITUDE);
        Schema roadSchema = new Schema(TrafficMonitoringConstants.Field.VEHICLE_ID, TrafficMonitoringConstants.Field.DATE_TIME, TrafficMonitoringConstants.Field.OCCUPIED, TrafficMonitoringConstants.Field.SPEED,
                TrafficMonitoringConstants.Field.BEARING, TrafficMonitoringConstants.Field.LATITUDE, TrafficMonitoringConstants.Field.LONGITUDE, TrafficMonitoringConstants.Field.ROAD_ID).asKey(TrafficMonitoringConstants.Field.ROAD_ID);
        Schema speedSchema = new Schema(TrafficMonitoringConstants.Field.NOW_DATE, TrafficMonitoringConstants.Field.ROAD_ID, TrafficMonitoringConstants.Field.AVG_SPEED, TrafficMonitoringConstants.Field.COUNT);
        
        Stream cars   = builder.createStream(TrafficMonitoringConstants.Streams.CARS, carSchema);
        Stream roads  = builder.createStream(TrafficMonitoringConstants.Streams.ROADS, roadSchema);
        Stream speeds = builder.createStream(TrafficMonitoringConstants.Streams.SPEEDS, speedSchema);
        
        builder.setSource(TrafficMonitoringConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(TrafficMonitoringConstants.Component.SOURCE, cars);
        
        builder.setOperator(TrafficMonitoringConstants.Component.MAP_MATCHER, new MapMatchingOperator(), mapMatcherThreads);
        builder.shuffle(TrafficMonitoringConstants.Component.MAP_MATCHER, cars);
        builder.publish(TrafficMonitoringConstants.Component.MAP_MATCHER, roads);
        
        builder.setOperator(TrafficMonitoringConstants.Component.SPEED_CALCULATOR, new SpeedCalculatorOperator(), speedCalcThreads);
        builder.groupByKey(TrafficMonitoringConstants.Component.SPEED_CALCULATOR, roads);
        builder.publish(TrafficMonitoringConstants.Component.SPEED_CALCULATOR, speeds);
        
        builder.setOperator(TrafficMonitoringConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(TrafficMonitoringConstants.Component.SINK, speeds);
    }
    
    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return TrafficMonitoringConstants.PREFIX;
    }

}
