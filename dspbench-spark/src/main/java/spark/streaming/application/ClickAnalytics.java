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
import spark.streaming.model.CountryStats;
import spark.streaming.model.Moving;
import spark.streaming.model.VisitStats;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;

import java.util.concurrent.TimeoutException;

public class ClickAnalytics extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalytics.class);
    private int parserThreads;
    private int repeatsThreads;
    private int geographyThreads;
    private int totalStatsThreads;
    private int geoStatsThreads;
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

        var repeats = records
                .repartition(repeatsThreads)
                .groupByKey((MapFunction<Row, String>) row -> row.get(1) + ":" + row.get(2), Encoders.STRING())
                .flatMapGroupsWithState(new SSFlatRepeatVisit(config), OutputMode.Append(), Encoders.BOOLEAN(), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

        var visitStats = repeats
                .repartition(totalStatsThreads)
                .groupByKey((MapFunction<Row, Integer>) row -> 0, Encoders.INT())
                .flatMapGroupsWithState(new SSVisitStats(config), OutputMode.Append(), Encoders.kryo(VisitStats.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

        var geos = records
                .repartition(geographyThreads)
                .map(new SSGeography(config), Encoders.kryo(Row.class));

        var geoStats = geos
                .repartition(geoStatsThreads)
                .filter(new SSFilterNull<>())
                .groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
                .flatMapGroupsWithState(new SSGeoStats(config), OutputMode.Append(), Encoders.kryo(CountryStats.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

        var visitS = createMultiSink(visitStats, visitSink, "visitSink", 1);
        var locationS = createMultiSink(geoStats, locationSink, "locationSink", 2);

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
