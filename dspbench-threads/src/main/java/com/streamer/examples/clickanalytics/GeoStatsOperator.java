package com.streamer.examples.clickanalytics;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.clickanalytics.ClickAnalyticsConstants.Field;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * User: domenicosolazzo
 */
public class GeoStatsOperator extends BaseOperator {
    private Map<String, CountryStats> stats;
    
    @Override
    public void initialize() {
        stats = new HashMap<String, CountryStats>();
    }

    public void process(Tuple tuple) {
        String country = tuple.getString(Field.COUNTRY);
        String city    = tuple.getString(Field.CITY);
        
        if (!stats.containsKey(country)) {
            stats.put(country, new CountryStats(country));
        }
        
        stats.get(country).cityFound(city);
        emit(tuple, new Values(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));
    }
    
    private class CountryStats implements Serializable {
        private int countryTotal = 0;

        private static final int COUNT_INDEX = 0;
        private static final int PERCENTAGE_INDEX = 1;
        
        private String countryName;
        private Map<String, List<Integer>> cityStats = new HashMap<String, List<Integer>>();

        public CountryStats(String countryName) {
            this.countryName = countryName;
        }
        
        public void cityFound(String cityName) {
            countryTotal++;
            
            if (cityStats.containsKey(cityName)) {
                cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX) + 1 );
            } else {
                List<Integer> list = new LinkedList<Integer>();
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
            return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
                    + "Cities: " + cityStats.toString();
        }
    }
}
