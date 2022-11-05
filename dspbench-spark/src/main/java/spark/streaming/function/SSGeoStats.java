package spark.streaming.function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.BaseConstants;
import spark.streaming.model.CountryStats;
import spark.streaming.util.Configuration;
import spark.streaming.util.geoip.IPLocation;
import spark.streaming.util.geoip.IPLocationFactory;
import spark.streaming.util.geoip.Location;

import java.util.HashMap;
import java.util.Map;

/**
 * @author luandopke
 */
public class SSGeoStats extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSGeoStats.class);

    private Map<String, CountryStats> stats;

    public SSGeoStats(Configuration config) {
        super(config);
        stats = new HashMap<>();
    }

    @Override
    public Row call(Row input) throws Exception {
        String country = input.getString(0);
        String city = input.getString(1);

        if (!stats.containsKey(country)) {
            stats.put(country, new CountryStats(country));
        }

        stats.get(country).cityFound(city);
        return RowFactory.create(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city));
    }
}