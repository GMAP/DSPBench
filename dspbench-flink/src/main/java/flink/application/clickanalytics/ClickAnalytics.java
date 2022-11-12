package flink.application.clickanalytics;

import flink.application.AbstractApplication;
import flink.constants.BaseConstants;
import flink.constants.ClickAnalyticsConstants;
import flink.parsers.ClickStreamParser;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickAnalytics extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalytics.class);

    private int repeatsThreads;
    private int geographyThreads;
    private int totalStatsThreads;
    private int geoStatsThreads;

    public ClickAnalytics(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        repeatsThreads       = config.getInteger(ClickAnalyticsConstants.Conf.REPEATS_THREADS, 1);
        geographyThreads     = config.getInteger(ClickAnalyticsConstants.Conf.GEOGRAPHY_THREADS, 1);
        totalStatsThreads    = config.getInteger(ClickAnalyticsConstants.Conf.TOTAL_STATS_THREADS, 1);
        geoStatsThreads      = config.getInteger(ClickAnalyticsConstants.Conf.GEO_STATS_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple4<String, String, String, String>> dataParse = data.map(new ClickStreamParser(config));

        // Process
        DataStream<Tuple4<String, String, String, String>> repVisit = dataParse.keyBy(
                new KeySelector<Tuple4<String, String, String, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple4<String, String, String, String> value) throws Exception {
                        return Tuple2.of(value.f1, value.f2);
                    }
                }
        ).filter(value -> (value != null)).flatMap(new RepeatVisit(config)).setParallelism(repeatsThreads);

        DataStream<Tuple3<String, String, String>> geo = dataParse.filter(value -> (value != null)).flatMap(new GeoFinder(config)).setParallelism(geographyThreads);

        DataStream<Tuple3<Integer, Integer, String>> visitStats = repVisit.flatMap(new VisitStats(config)).setParallelism(totalStatsThreads);

        DataStream<Tuple5<String, Integer, String, Integer, String>> geoStats = geo.keyBy(value -> value.f0).flatMap(new GeoStats(config)).setParallelism(geoStatsThreads).keyBy(value -> value.f0);

        // Sink
        // YEAHH, could reuse some sinks
        createSinkLPStatus(visitStats, "visit");
        createSinkLPGeo(geoStats, "location");

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return ClickAnalyticsConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
