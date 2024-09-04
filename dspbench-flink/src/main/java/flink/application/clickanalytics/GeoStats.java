package flink.application.clickanalytics;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GeoStats extends RichFlatMapFunction<Tuple2<String, String>, Tuple4<String, Integer, String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(GeoStats.class);

    private static Map<String, CountryStats> stats;

    Configuration config;

    Metrics metrics = new Metrics();

    public GeoStats(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    private Map<String, CountryStats>  getStats() {
        if (stats == null) {
            stats = new HashMap<>();
        }

        return stats;
    }

    @Override
    public void flatMap(Tuple2<String, String> input, Collector<Tuple4<String, Integer, String, Integer>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.recemitThroughput();
        }
        getStats();
        String country = input.getField(0);
        String city    = input.getField(1);

        if (!stats.containsKey(country)) {
            stats.put(country, new CountryStats(country));
        }

        stats.get(country).cityFound(city);
        //LOG.info(input.toString());
        out.collect( new Tuple4<String, Integer, String, Integer>(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));
    }

    private class CountryStats {
        private int countryTotal = 0;

        private static final int COUNT_INDEX = 0;
        private static final int PERCENTAGE_INDEX = 1;

        private final String countryName;
        private final Map<String, List<Integer>> cityStats = new HashMap<>();

        public CountryStats(String countryName) {
            this.countryName = countryName;
        }

        public void cityFound(String cityName) {
            countryTotal++;

            if (cityStats.containsKey(cityName)) {
                cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX) + 1 );
            } else {
                List<Integer> list = new LinkedList<>();
                list.add(1);
                list.add(0);
                cityStats.put(cityName, list);
            }

            double percent = (double)cityStats.get(cityName).get(COUNT_INDEX)/(double)countryTotal;
            cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
        }

        public int getCountryTotal() {
            return countryTotal;
        }

        public int getCityTotal(String cityName) {
            return cityStats.get(cityName).get(COUNT_INDEX);
        }

        @Override
        public String toString() {
            return "Total Count for " + countryName + " is " + countryTotal + "\n"
                    + "Cities: " + cityStats;
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
