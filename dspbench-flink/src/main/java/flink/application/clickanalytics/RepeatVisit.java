package flink.application.clickanalytics;

import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.application.trafficmonitoring.gis.RoadGridList;
import flink.constants.SentimentAnalysisConstants;
import flink.constants.TrafficMonitoringConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class RepeatVisit implements FlatMapFunction<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(RepeatVisit.class);

    private static Map<String, Void> map;

    public RepeatVisit(Configuration config) {
        getVisits();
    }

    private Map<String, Void>  getVisits() {
        if (map == null) {
            map = new HashMap<>();
        }

        return map;
    }


    @Override
    public void flatMap(Tuple4<String, String, String, String> input, Collector<Tuple4<String, String, String, String>> out) {
        getVisits();

        String clientKey = input.getField(2);
        String url = input.getField(1);
        String key = url + ":" + clientKey;
        String initTime = input.getField(3);

        if (map.containsKey(key)) {
            out.collect(new Tuple4<String, String, String,String>(clientKey, url, Boolean.FALSE.toString(), initTime));
        } else {
            map.put(key, null);
        out.collect(new Tuple4<String, String, String,String>(clientKey, url, Boolean.TRUE.toString(), initTime));
        }
        //super.calculateThroughput();
    }
}
