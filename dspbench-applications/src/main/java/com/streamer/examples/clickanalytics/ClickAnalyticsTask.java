package com.streamer.examples.clickanalytics;

import com.streamer.base.sink.BaseSink;
import com.streamer.base.source.BaseSource;
import com.streamer.base.task.AbstractTask;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import static com.streamer.examples.clickanalytics.ClickAnalyticsConstants.*;
import com.streamer.partitioning.Fields;
import com.streamer.utils.Configuration;
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
        
        repeatsThreads       = config.getInt(Config.REPEATS_THREADS, 1);
        geographyThreads     = config.getInt(Config.GEOGRAPHY_THREADS, 1);
        geoStatsThreads      = config.getInt(Config.GEO_STATS_THREADS, 1);
        sourceThreads         = config.getInt(Config.SOURCE_THREADS, 1);
        visitSinkThreads     = config.getInt(getConfigKey(Config.SINK_THREADS, "visit"), 1);
        locationSinkThreads  = config.getInt(getConfigKey(Config.SINK_THREADS, "location"), 1);
        
        source       = loadSource();
        visitSink    = loadSink("visit");
        locationSink = loadSink("location");
    }

    public void initialize() {
        Stream clicks = builder.createStream(Streams.CLICKS,
                new Schema(Field.IP, Field.URL, Field.CLIENT_KEY));
        Stream visits = builder.createStream(Streams.VISITS,
                new Schema(Field.CLIENT_KEY, Field.UNIQUE));
        Stream locations = builder.createStream(Streams.LOCATIONS,
                new Schema().keys(Field.COUNTRY).fields(Field.CITY));
        Stream totalVisits = builder.createStream(Streams.TOTAL_VISITS, 
                new Schema(Field.TOTAL_COUNT, Field.TOTAL_UNIQUE));
        Stream totalLocations = builder.createStream(Streams.TOTAL_LOCATIONS,
                new Schema().keys(Field.COUNTRY).fields(Field.COUNTRY_TOTAL, Field.CITY, Field.CITY_TOTAL));
        
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish("source", clicks);
        //builder.setTupleRate("source", sourceRate);
        
        
        builder.setOperator(Component.REPEATS, new RepeatVisitOperator(), repeatsThreads);
        builder.groupBy(Component.REPEATS, clicks, new Fields(Field.URL, Field.CLIENT_KEY));
        builder.publish(Component.REPEATS, visits);
        
        builder.setOperator(Component.TOTAL_STATS, new VisitStatsOperator(), 1);
        builder.bcast(Component.TOTAL_STATS, visits);
        builder.publish(Component.TOTAL_STATS, totalVisits);
        
        builder.setOperator(Component.GEOGRAPHY, new GeographyOperator(), geographyThreads);
        builder.shuffle(Component.GEOGRAPHY, clicks);
        builder.publish(Component.GEOGRAPHY, locations);
        
        builder.setOperator(Component.GEO_STATS, new GeoStatsOperator(), geoStatsThreads);
        builder.groupByKey(Component.GEO_STATS, locations);
        builder.publish(Component.GEO_STATS, totalLocations);
        
        
        builder.setOperator(Component.SINK_VISIT, visitSink, visitSinkThreads);
        builder.shuffle(Component.SINK_VISIT, totalVisits);
        
        builder.setOperator(Component.SINK_LOCATION, locationSink, locationSinkThreads);
        builder.shuffle(Component.SINK_LOCATION, totalLocations);
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}
