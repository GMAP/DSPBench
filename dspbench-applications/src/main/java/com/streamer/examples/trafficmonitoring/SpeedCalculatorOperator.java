package com.streamer.examples.trafficmonitoring;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.trafficmonitoring.TrafficMonitoringConstants.Field;
import com.streamer.examples.trafficmonitoring.gis.Road;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author mayconbordin
 */
public class SpeedCalculatorOperator extends BaseOperator {
    private Map<Integer, Road> roads;
    
    @Override
    public void initialize() {
        roads = new HashMap<Integer, Road>();
    }
    
    @Override
    public void process(Tuple input) {
        int roadID = input.getInt(Field.ROAD_ID);
        int speed  = input.getInt(Field.SPEED);
        
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
        
        emit(input, new Values(new Date(), roadID, averageSpeed, count));
    }
    
}
