package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Configurations;
import flink.util.Metrics;

public class SmartPlugParser extends RichFlatMapFunction<String, Tuple7<String, Long, Double, Integer, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SmartPlugParser.class);

    Configuration config;

    Metrics metrics = new Metrics();

    public SmartPlugParser(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple7<String, Long, Double, Integer, String, String, String>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String[] temp = input.split(",");

        if (temp.length == 7){
            try{
                String id = temp[0];
                long timestamp = Long.parseLong(temp[1]);
                double value = Double.parseDouble(temp[2]);
                int property = Integer.parseInt(temp[3]);
                String plugId = temp[4];
                String householdId = temp[5];
                String houseId = temp[6];
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                out.collect(new Tuple7<>(id, timestamp, value, property, plugId, householdId, houseId));

            } catch (NumberFormatException ex) {
                System.out.println("Error parsing numeric value " + ex);
            }
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
