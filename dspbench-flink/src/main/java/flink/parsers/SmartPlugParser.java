package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartPlugParser extends Parser implements MapFunction<String, Tuple7<String, Long, Double, Integer, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SmartPlugParser.class);

    Configuration config;

    public SmartPlugParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple7<String, Long, Double, Integer, String, String, String> map(String input) throws Exception {
        super.initialize(config);
        super.incReceived();
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
            super.incEmitted();
            return new Tuple7<>(id, timestamp, value, property, plugId, householdId, houseId);

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
