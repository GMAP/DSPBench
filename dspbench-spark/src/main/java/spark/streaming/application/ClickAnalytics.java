package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.ClickAnalyticsConstants;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.function.*;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;

import java.util.concurrent.TimeoutException;

public class ClickAnalytics extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalytics.class);
    private int parserThreads;
    private int repeatsThreads;
    private int geographyThreads;
    private int totalStatsThreads;
    private int geoStatsThreads;
    private int spoutThreads;
    private int visitSinkThreads;
    private int locationSinkThreads;
    private String visitSink;
    private String locationSink;

    public ClickAnalytics(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(ClickAnalyticsConstants.Config.PARSER_THREADS, 1);
        repeatsThreads = config.getInt(ClickAnalyticsConstants.Config.REPEATS_THREADS, 1);
        geographyThreads = config.getInt(ClickAnalyticsConstants.Config.GEOGRAPHY_THREADS, 1);
        totalStatsThreads = config.getInt(ClickAnalyticsConstants.Config.TOTAL_STATS_THREADS, 1);
        geoStatsThreads = config.getInt(ClickAnalyticsConstants.Config.GEO_STATS_THREADS, 1);
        visitSink = config.get(ClickAnalyticsConstants.Component.SINK_VISIT);
        locationSink = config.get(ClickAnalyticsConstants.Component.SINK_LOCATION);
    }

    @Override
    public DataStreamWriter buildApplication() throws StreamingQueryException {
        var rawRecords = createSource();

        var records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new ClickStreamParser(config), Encoders.kryo(Row.class));

        //TODO can join two operators in one, using groupbykey and mapgroupswithstate.
        var repeats = records
                .repartition(repeatsThreads)
                .map(new SSRepeatVisit(config), Encoders.kryo(Row.class));

        var visitStats = repeats
                .repartition(totalStatsThreads)
                .map(new SSVisitStats(config), Encoders.kryo(Row.class));


        var geos = records
                .repartition(geographyThreads)
                .map(new SSGeography(config), Encoders.kryo(Row.class));

        var geoStats = geos
                .repartition(geoStatsThreads)
                .filter(new SSFilterNull<>())
                .map(new SSGeoStats(config), Encoders.kryo(Row.class));


        var visitS = createMultiSink(visitStats, visitSink);
        var locationS = createMultiSink(geoStats, locationSink);

        visitS.awaitTermination();
        locationS.awaitTermination();

        return createSink();
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
