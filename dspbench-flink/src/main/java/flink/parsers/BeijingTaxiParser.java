package flink.parsers;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class BeijingTaxiParser extends RichFlatMapFunction<String, Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>> {

    Configuration config;
    Metrics metrics = new Metrics();
    private static final Logger LOG = LoggerFactory.getLogger(BeijingTaxiParser.class);

    public BeijingTaxiParser(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] temp = input.split(",");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

        if (temp.length == 7){
            try {
                String carId = temp[0];
                DateTime date = formatter.parseDateTime(temp[2]);
                boolean occ = true;
                double lat = Double.parseDouble(temp[3]);
                double lon = Double.parseDouble(temp[4]);
                int speed = ((Double) Double.parseDouble(temp[5])).intValue();
                int bearing = Integer.parseInt(temp[6]);
    
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
    
                out.collect(new Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>(carId, date, occ, speed, bearing, lat, lon));
    
            } catch (NumberFormatException ex) {
                System.out.println("Error parsing numeric value " + ex);
            } catch (IllegalArgumentException ex) {
                System.out.println("Error parsing date/time value   " + ex);
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
