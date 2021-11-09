package com.streamer.examples.trafficmonitoring;

import com.streamer.base.task.BasicTask;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import static com.streamer.examples.trafficmonitoring.TrafficMonitoringConstants.*;
import com.streamer.examples.trafficmonitoring.TrafficMonitoringConstants.Config;
import com.streamer.examples.trafficmonitoring.TrafficMonitoringConstants.Streams;
import com.streamer.utils.Configuration;
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
        
        mapMatcherThreads = config.getInt(Config.MAP_MATCHER_THREADS, 1);
        speedCalcThreads  = config.getInt(Config.SPEED_CALCULATOR_THREADS, 1);
    }
    
    public void initialize() {
        Schema carSchema = new Schema(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED,
                Field.SPEED, Field.BEARING, Field.LATITUDE, Field.LONGITUDE);
        Schema roadSchema = new Schema(Field.VEHICLE_ID, Field.DATE_TIME, Field.OCCUPIED, Field.SPEED,
                Field.BEARING, Field.LATITUDE, Field.LONGITUDE, Field.ROAD_ID).asKey(Field.ROAD_ID);
        Schema speedSchema = new Schema(Field.NOW_DATE, Field.ROAD_ID, Field.AVG_SPEED, Field.COUNT);
        
        Stream cars   = builder.createStream(Streams.CARS, carSchema);
        Stream roads  = builder.createStream(Streams.ROADS, roadSchema);
        Stream speeds = builder.createStream(Streams.SPEEDS, speedSchema);
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, cars);
        
        builder.setOperator(Component.MAP_MATCHER, new MapMatchingOperator(), mapMatcherThreads);
        builder.shuffle(Component.MAP_MATCHER, cars);
        builder.publish(Component.MAP_MATCHER, roads);
        
        builder.setOperator(Component.SPEED_CALCULATOR, new SpeedCalculatorOperator(), speedCalcThreads);
        builder.groupByKey(Component.SPEED_CALCULATOR, roads);
        builder.publish(Component.SPEED_CALCULATOR, speeds);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, speeds);
    }
    
    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

}
