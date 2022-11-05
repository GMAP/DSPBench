package flink.parsers;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class SmartPlugParser extends Parser implements MapFunction<String, Tuple8<String, Long, Double, Integer, String, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SmartPlugParser.class);

    @Override
    public Tuple8<String, Long, Double, Integer, String, String, String, String> map(String input) throws Exception {
        super.calculateThroughput();
        String[] temp = input.split(",");

        if (temp.length != 7)
            return null;

        try{
            String id = temp[0];
            long timestamp = Long.parseLong(temp[1]);
            double value = Double.parseDouble(temp[2]);
            int property = Integer.parseInt(temp[3]);
            String plugId = temp[4];
            String householdId = temp[5];
            String houseId = temp[6];

            return new Tuple8<>(id, timestamp, value, property, plugId, householdId, houseId, Instant.now().toEpochMilli() + "");

        } catch (NumberFormatException ex) {
            System.out.println("Error parsing numeric value " + ex);
        }

        return null;
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
