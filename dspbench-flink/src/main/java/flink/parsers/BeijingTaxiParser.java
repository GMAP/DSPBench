package flink.parsers;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.time.Instant;

/**
 *
 */
public class BeijingTaxiParser extends Metrics implements MapFunction<String, Tuple8<String, DateTime, Boolean,Integer, Integer, Double, Double, String>> {

    Configuration config;

    public BeijingTaxiParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple8<String, DateTime, Boolean,Integer, Integer, Double, Double, String> map(String value) throws Exception {
        super.initialize(config);
        super.calculateThroughput();

        String[] temp = value.split(",");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

        if (temp.length != 7)
            return null;

        try {
            String carId  = temp[0];
            DateTime date = formatter.parseDateTime(temp[2]);
            boolean occ   = true;
            double lat    = Double.parseDouble(temp[3]);
            double lon    = Double.parseDouble(temp[4]);
            int speed     = ((Double)Double.parseDouble(temp[5])).intValue();
            int bearing   = Integer.parseInt(temp[6]);

            return new Tuple8<>(carId, date, occ, speed, bearing, lat, lon, Instant.now().toEpochMilli() + "");

        } catch (NumberFormatException ex) {
            System.out.println("Error parsing numeric value " + ex);
        } catch (IllegalArgumentException ex) {
            System.out.println("Error parsing date/time value   " + ex);
        }
        return null;
    }
    
}
