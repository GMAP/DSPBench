package spark.streaming.function;

import com.google.common.base.Optional;
import java.util.Date;
import java.util.List;
import org.apache.spark.api.java.function.Function2;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class SpeedCalculator extends BaseFunction implements Function2<List<Tuple>, Optional<Object>, Optional<Object>> {

    public SpeedCalculator(Configuration config) {
        super(config);
    }
    
    @Override
    public Optional<Tuple> call(List<Tuple> values, Optional<Tuple> state) throws Exception {
        incReceived(values.size());
        
        Tuple newState = state.or(new Tuple(values));
        newState.set("timestamp", new Date());
        
        if (newState.get("roadID") == null && values.size() > 0) {
            newState.set("roadID", values.get(0).get("roadID"));
        }
        
        for (Tuple value : values) {
            updateTuple(newState, value);
        }
        
        incEmitted();
        return Optional.of(newState);
    }
    
    private void updateTuple(Tuple state, Tuple tuple) {
        int roadID = tuple.getInt("roadID");
        int speed  = tuple.getInt("speed");
        
        int averageSpeed = 0;
        int count = 0;
        
        if (state.get("road") == null) {
            Road road = new Road(roadID);
            road.addRoadSpeed(speed);
            road.setCount(1);
            road.setAverageSpeed(speed);
            
            state.set("road", road);
            averageSpeed = speed;
            count = 1;
        } else {
            Road road = (Road) state.get("road");
            
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
                double avgLast = road.getAverageSpeed();
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
        
        state.set("averageSpeed", averageSpeed);
        state.set("count", count);
    }
}