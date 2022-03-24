package spark.streaming.application;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import static spark.streaming.constants.LogProcessingConstants.*;
import spark.streaming.function.CityCount;
import spark.streaming.function.CitySingleCounter;
import spark.streaming.function.CommonLogParser;
import spark.streaming.function.CountPerMinute;
import spark.streaming.function.CountSingleStatus;
import spark.streaming.function.CountStatus;
import spark.streaming.function.CountVolume;
import spark.streaming.function.CountryCount;
import spark.streaming.function.FilterNull;
import spark.streaming.function.GeoFinder;
import spark.streaming.sink.PairSink;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class LogProcessing extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(LogProcessing.class);
    
    private int batchSize;
    private int parserThreads;
    private int singleVolumeCounterThreads;
    private int volumeCounterThreads;
    private int singleStatusCounterThreads;
    private int statusCounterThreads;
    private int geoFinderThreads;
    private int countryCountThreads;
    private int citySingleCounterThreads;
    private int cityCounterThreads;
    
    public LogProcessing(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        batchSize                  = config.getInt(getConfigKey(Config.BATCH_SIZE), 1000);
        parserThreads              = config.getInt(Config.PARSER_THREADS, 1);
        
        singleVolumeCounterThreads = config.getInt(Config.SINGLE_VOL_COUNTER_THREADS, 1);
        volumeCounterThreads       = config.getInt(Config.VOLUME_COUNTER_THREADS, 1);
        
        singleStatusCounterThreads = config.getInt(Config.SINGLE_STATUS_COUNTER_THREADS, 1);
        statusCounterThreads       = config.getInt(Config.STATUS_COUNTER_THREADS, 1);
        
        geoFinderThreads           = config.getInt(Config.GEO_FINDER_THREADS, 1);
        countryCountThreads        = config.getInt(Config.COUNTRY_COUNTER_THREADS, 1);
        
        citySingleCounterThreads   = config.getInt(Config.CITY_SINGLE_COUNTER_THREADS, 1);
        cityCounterThreads         = config.getInt(Config.CITY_COUNTER_THREADS, 1);
    }

    @Override
    public JavaStreamingContext buildApplication() {
        context = new JavaStreamingContext(config, new Duration(batchSize));
                
        JavaDStream<Tuple2<String, Tuple>> rawLogs = createSource();
        
        JavaDStream<Tuple2<Long, Tuple>> logs = rawLogs.repartition(parserThreads)
                .map(new CommonLogParser(config));
        
        
        // volume counter
        JavaPairDStream<Long, Tuple> singleVolumeCounts = logs.repartition(singleVolumeCounterThreads)
                .mapToPair(new CountPerMinute(config));
        
        JavaPairDStream<Long, Tuple> volumeCounts = singleVolumeCounts.reduceByKey(
                new CountVolume(config), volumeCounterThreads);
        
        
        // status counter
        JavaPairDStream<Integer, Tuple> singleStatusCounts = logs.repartition(singleStatusCounterThreads)
                .mapToPair(new CountSingleStatus(config));
        
        JavaPairDStream<Integer, Tuple> statusCounts = singleStatusCounts.reduceByKey(
                new CountStatus(config), statusCounterThreads);
        
        
        // geography counters: country and city
        JavaPairDStream<String, Tuple> locations = logs.repartition(geoFinderThreads)
                .mapToPair(new GeoFinder(config));
        
        JavaPairDStream<String, Tuple> countryCounts = locations.filter(new FilterNull<String, Tuple>())
                .reduceByKey(new CountryCount(config), countryCountThreads);
        
        
        JavaPairDStream<String, Tuple> citySingleCounts = locations.filter(new FilterNull<String, Tuple>())
                .repartition(citySingleCounterThreads)
                .mapToPair(new CitySingleCounter(config));
        
        JavaPairDStream<String, Tuple> cityCounts = citySingleCounts.filter(new FilterNull<String, Tuple>())
                .reduceByKey(new CityCount(config), cityCounterThreads);
        
        // sinks
        volumeCounts.foreachRDD(new PairSink<Long>(config, "volumeCountsSink"));
        statusCounts.foreachRDD(new PairSink<Integer>(config, "statusCountsSink"));
        countryCounts.foreachRDD(new PairSink<String>(config, "countryCountsSink"));
        cityCounts.foreachRDD(new PairSink<String>(config, "cityCountsSink"));
        
        return context;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
