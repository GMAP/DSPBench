package org.dspbench.applications.trafficmonitoring;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.dspbench.applications.trafficmonitoring.TrafficMonitoringConstants.Field;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.dspbench.applications.trafficmonitoring.gis.Road;

/**
 * Copyright 2013 Xdata@SIAT
 * email: gh.chen@siat.ac.cn 
 */
public class SpeedCalculatorBolt extends AbstractBolt {
    private Map<Integer, Road> roads;

    @Override
    public void initialize() {
        roads = new HashMap<>();
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }
    
    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }
        int roadID = input.getIntegerByField(Field.ROAD_ID);
        int speed  = input.getIntegerByField(Field.SPEED);

        int averageSpeed = 0;
        int count = 0;
        
        if (!roads.containsKey(roadID)) {
            Road road = new Road(roadID);
            road.addRoadSpeed(speed);
            road.setCount(1);
            road.setAverageSpeed(speed);
            
            roads.put(roadID, road);
            averageSpeed = speed;
            count = 1;
        } else {
            Road road = roads.get(roadID);
            
            int sum = 0;
            
            if (road.getRoadSpeedSize() < 2) {
                road.incrementCount();
                road.addRoadSpeed(speed);
                
                for (int it : road.getRoadSpeed()) {
                    sum += it;
                }
                
                averageSpeed = (int)((double)sum/(double)road.getRoadSpeedSize());
                road.setAverageSpeed(averageSpeed);
                count = road.getRoadSpeedSize();
            } else {
                double avgLast = roads.get(roadID).getAverageSpeed();
                double temp = 0;
                
                for (int it : road.getRoadSpeed()) {
                    sum += it;
                    temp += Math.pow((it-avgLast), 2);
                }
                
                int avgCurrent = (int) ((sum + speed)/((double)road.getRoadSpeedSize() + 1));
                temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                double stdDev = Math.sqrt(temp);
                
                if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);
                    road.setAverageSpeed(avgCurrent);
                    
                    averageSpeed = avgCurrent;
                    count = road.getRoadSpeedSize();
                }
            }
        }
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            emittedThroughput();
        }
        collector.emit(input, new Values(new Date(), roadID, averageSpeed, count, input.getStringByField(TrafficMonitoringConstants.Field.INITTIME)));
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.NOW_DATE, Field.ROAD_ID, Field.AVG_SPEED, Field.COUNT, Field.INITTIME);
    }
}
