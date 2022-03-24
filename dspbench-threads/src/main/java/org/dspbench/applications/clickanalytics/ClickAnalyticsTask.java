package org.dspbench.applications.clickanalytics;

import org.dspbench.base.sink.BaseSink;
import org.dspbench.base.source.BaseSource;
import org.dspbench.base.task.AbstractTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.partitioning.Fields;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickAnalyticsTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalyticsTask.class);
    
    private int sourceThreads;
    private int repeatsThreads;
    private int geographyThreads;
    private int geoStatsThreads;
    private int visitSinkThreads;
    private int locationSinkThreads;
    
    private BaseSource source;
    private BaseSink visitSink;
    private BaseSink locationSink;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        repeatsThreads       = config.getInt(ClickAnalyticsConstants.Config.REPEATS_THREADS, 1);
        geographyThreads     = config.getInt(ClickAnalyticsConstants.Config.GEOGRAPHY_THREADS, 1);
        geoStatsThreads      = config.getInt(ClickAnalyticsConstants.Config.GEO_STATS_THREADS, 1);
        sourceThreads         = config.getInt(ClickAnalyticsConstants.Config.SOURCE_THREADS, 1);
        visitSinkThreads     = config.getInt(getConfigKey(ClickAnalyticsConstants.Config.SINK_THREADS, "visit"), 1);
        locationSinkThreads  = config.getInt(getConfigKey(ClickAnalyticsConstants.Config.SINK_THREADS, "location"), 1);
        
        source       = loadSource();
        visitSink    = loadSink("visit");
        locationSink = loadSink("location");
    }

    public void initialize() {
        Stream clicks = builder.createStream(ClickAnalyticsConstants.Streams.CLICKS,
                new Schema(ClickAnalyticsConstants.Field.IP, ClickAnalyticsConstants.Field.URL, ClickAnalyticsConstants.Field.CLIENT_KEY));
        Stream visits = builder.createStream(ClickAnalyticsConstants.Streams.VISITS,
                new Schema(ClickAnalyticsConstants.Field.CLIENT_KEY, ClickAnalyticsConstants.Field.UNIQUE));
        Stream locations = builder.createStream(ClickAnalyticsConstants.Streams.LOCATIONS,
                new Schema().keys(ClickAnalyticsConstants.Field.COUNTRY).fields(ClickAnalyticsConstants.Field.CITY));
        Stream totalVisits = builder.createStream(ClickAnalyticsConstants.Streams.TOTAL_VISITS,
                new Schema(ClickAnalyticsConstants.Field.TOTAL_COUNT, ClickAnalyticsConstants.Field.TOTAL_UNIQUE));
        Stream totalLocations = builder.createStream(ClickAnalyticsConstants.Streams.TOTAL_LOCATIONS,
                new Schema().keys(ClickAnalyticsConstants.Field.COUNTRY).fields(ClickAnalyticsConstants.Field.COUNTRY_TOTAL, ClickAnalyticsConstants.Field.CITY, ClickAnalyticsConstants.Field.CITY_TOTAL));
        
        
        builder.setSource(ClickAnalyticsConstants.Component.SOURCE, source, sourceThreads);
        builder.publish("source", clicks);
        //builder.setTupleRate("source", sourceRate);
        
        
        builder.setOperator(ClickAnalyticsConstants.Component.REPEATS, new RepeatVisitOperator(), repeatsThreads);
        builder.groupBy(ClickAnalyticsConstants.Component.REPEATS, clicks, new Fields(ClickAnalyticsConstants.Field.URL, ClickAnalyticsConstants.Field.CLIENT_KEY));
        builder.publish(ClickAnalyticsConstants.Component.REPEATS, visits);
        
        builder.setOperator(ClickAnalyticsConstants.Component.TOTAL_STATS, new VisitStatsOperator(), 1);
        builder.bcast(ClickAnalyticsConstants.Component.TOTAL_STATS, visits);
        builder.publish(ClickAnalyticsConstants.Component.TOTAL_STATS, totalVisits);
        
        builder.setOperator(ClickAnalyticsConstants.Component.GEOGRAPHY, new GeographyOperator(), geographyThreads);
        builder.shuffle(ClickAnalyticsConstants.Component.GEOGRAPHY, clicks);
        builder.publish(ClickAnalyticsConstants.Component.GEOGRAPHY, locations);
        
        builder.setOperator(ClickAnalyticsConstants.Component.GEO_STATS, new GeoStatsOperator(), geoStatsThreads);
        builder.groupByKey(ClickAnalyticsConstants.Component.GEO_STATS, locations);
        builder.publish(ClickAnalyticsConstants.Component.GEO_STATS, totalLocations);
        
        
        builder.setOperator(ClickAnalyticsConstants.Component.SINK_VISIT, visitSink, visitSinkThreads);
        builder.shuffle(ClickAnalyticsConstants.Component.SINK_VISIT, totalVisits);
        
        builder.setOperator(ClickAnalyticsConstants.Component.SINK_LOCATION, locationSink, locationSinkThreads);
        builder.shuffle(ClickAnalyticsConstants.Component.SINK_LOCATION, totalLocations);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return ClickAnalyticsConstants.PREFIX;
    }
}
