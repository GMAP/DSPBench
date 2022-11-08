package flink.application.trafficmonitoring;

import flink.application.trafficmonitoring.gis.GPSRecord;
import flink.application.trafficmonitoring.gis.Road;
import flink.application.trafficmonitoring.gis.RoadGridList;
import flink.constants.TrafficMonitoringConstants;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SpeedCalculator extends Metrics implements FlatMapFunction<Tuple9<String, DateTime, Boolean,Integer, Integer, Double, Double, Integer, String>, Tuple5<Date, Integer, Integer, Integer, String>>  {

    private static final Logger LOG = LoggerFactory.getLogger(SpeedCalculator.class);

    private Map<Integer, Road> roads;

    Configuration config;

    public SpeedCalculator(Configuration config) {
        this.config = config;
        super.initialize(config);
        roads = new HashMap<>();
    }

    @Override
    public void flatMap(Tuple9<String, DateTime, Boolean,Integer, Integer, Double, Double, Integer, String> input, Collector<Tuple5<Date, Integer, Integer, Integer, String>> out){
        super.initialize(config);
        int roadID = input.getField(7);
        int speed  = input.getField(3);

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

        out.collect(new Tuple5<Date, Integer, Integer, Integer, String>(new Date(), roadID, averageSpeed, count, input.f8));
        super.calculateThroughput();
    }
}
