package com.streamer.examples.logprocessing;

import com.streamer.base.sink.BaseSink;
import com.streamer.base.source.BaseSource;
import com.streamer.base.task.AbstractTask;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import com.streamer.examples.clickanalytics.GeoStatsOperator;
import com.streamer.examples.clickanalytics.GeographyOperator;
import static com.streamer.examples.logprocessing.LogProcessingConstants.*;
import com.streamer.partitioning.Fields;
import com.streamer.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * https://github.com/ashrithr/LogEventsProcessing
 * @author Ashrith Mekala <ashrith@me.com>
 */
public class LogProcessingTask extends AbstractTask {
    private static final Logger LOG = LoggerFactory.getLogger(LogProcessingTask.class);
    
    private BaseSource source;
    private BaseSink volumeSink;
    private BaseSink statusSink;
    private BaseSink countrySink;
    
    private int sourceThreads;
    private int volumeSinkThreads;
    private int statusSinkThreads;
    private int countrySinkThreads;
    private int volumeCountThreads;
    private int statusCountThreads;
    private int geoFinderThreads;
    private int geoStatsThreads;
    
    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        source      = loadSource();
        volumeSink   = loadSink("volume");
        statusSink  = loadSink("status");
        countrySink = loadSink("country");
        
        sourceThreads       = config.getInt(getConfigKey(Config.SOURCE_THREADS), 1);
        volumeSinkThreads   = config.getInt(getConfigKey(Config.SINK_THREADS, "volume"), 1);
        statusSinkThreads  = config.getInt(getConfigKey(Config.SINK_THREADS, "status"), 1);
        countrySinkThreads = config.getInt(getConfigKey(Config.SINK_THREADS, "country"), 1);
        
        volumeCountThreads = config.getInt(Config.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInt(Config.STATUS_COUNTER_THREADS, 1);
        geoFinderThreads   = config.getInt(Config.GEO_FINDER_THREADS, 1);
        geoStatsThreads    = config.getInt(Config.GEO_STATS_THREADS, 1);
    }

    public void initialize() {
        Stream logs = builder.createStream(Streams.LOGS, new Schema()
                .keys(Field.IP, Field.TIMESTAMP_MINUTES, Field.RESPONSE_CODE)
                .fields(Field.TIMESTAMP, Field.REQUEST, Field.BYTE_SIZE));
        Stream statusCounts = builder.createStream(Streams.STATUS_COUNTS, 
                new Schema(Field.RESPONSE_CODE, Field.COUNT));
        Stream locations = builder.createStream(Streams.LOCATIONS, 
                new Schema().keys(Field.COUNTRY).fields(Field.CITY));
        Stream locationCounts = builder.createStream(Streams.LOCATION_COUNTS, 
                new Schema().keys(Field.COUNTRY).fields(Field.COUNTRY_TOTAL, Field.CITY, Field.CITY_TOTAL));
        Stream volumeCounts = builder.createStream(Streams.VOLUME_COUNTS, 
                new Schema(Field.TIMESTAMP_MINUTES, Field.COUNT));
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, logs);
        //builder.setTupleRate(Component.SOURCE, sourceRate);
        
        
        builder.setOperator(Component.VOLUME_COUNTER, new VolumeCountOperator(), volumeCountThreads);
        builder.groupBy(Component.VOLUME_COUNTER, logs, new Fields(Field.TIMESTAMP_MINUTES));
        builder.publish(Component.VOLUME_COUNTER, volumeCounts);
        
        builder.setOperator(Component.STATUS_COUNTER, new StatusCountOperator(), statusCountThreads);
        builder.groupBy(Component.STATUS_COUNTER, logs, new Fields(Field.RESPONSE_CODE));
        builder.publish(Component.STATUS_COUNTER, statusCounts);
        
        builder.setOperator(Component.GEO_FINDER, new GeographyOperator(), geoFinderThreads);
        builder.shuffle(Component.GEO_FINDER, logs);
        builder.publish(Component.GEO_FINDER, locations);
        
        builder.setOperator(Component.GEO_STATS, new GeoStatsOperator(), geoStatsThreads);
        builder.groupByKey(Component.GEO_STATS, locations);
        builder.publish(Component.GEO_STATS, locationCounts);
        
        
        builder.setOperator(Component.VOLUME_SINK, volumeSink, volumeSinkThreads);
        builder.shuffle(Component.VOLUME_SINK, volumeCounts);
        
        builder.setOperator(Component.STATUS_SINK, statusSink, statusSinkThreads);
        builder.shuffle(Component.STATUS_SINK, statusCounts);
        
        builder.setOperator(Component.GEO_SINK, countrySink, countrySinkThreads);
        builder.shuffle(Component.GEO_SINK, locationCounts);
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
