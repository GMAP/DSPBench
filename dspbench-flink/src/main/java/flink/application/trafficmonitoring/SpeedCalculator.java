package flink.application.trafficmonitoring;

import flink.application.trafficmonitoring.gis.Road;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SpeedCalculator extends RichFlatMapFunction<Tuple8<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer>, Tuple4<Date, Integer, Integer, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(SpeedCalculator.class);

    private final Map<Integer, Road> roads;

    Configuration config;

    Metrics metrics = new Metrics();

    public SpeedCalculator(Configuration config) {
        this.config = config;
        metrics.initialize(config, this.getClass().getSimpleName());
        this.roads = new HashMap<>();
    }

    @Override
    public void flatMap(Tuple8<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer> input,
            Collector<Tuple4<Date, Integer, Integer, Integer>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());    
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        int roadID = input.getField(7);
        int speed = input.getField(3);

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

                averageSpeed = (int) ((double) sum / (double) road.getRoadSpeedSize());
                road.setAverageSpeed(averageSpeed);
                count = road.getRoadSpeedSize();
            } else {
                double avgLast = roads.get(roadID).getAverageSpeed();
                double temp = 0;

                for (int it : road.getRoadSpeed()) {
                    sum += it;
                    temp += Math.pow((it - avgLast), 2);
                }

                int avgCurrent = (int) ((sum + speed) / ((double) road.getRoadSpeedSize() + 1));
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

        out.collect(new Tuple4<Date, Integer, Integer, Integer>(new Date(), roadID, averageSpeed, count));

    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
