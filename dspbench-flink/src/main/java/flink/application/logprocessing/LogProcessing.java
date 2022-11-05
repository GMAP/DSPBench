package flink.application.logprocessing;

import flink.application.AbstractApplication;
import flink.application.sentimentanalysis.SentimentCalculator;
import flink.constants.LogProcessingConstants;
import flink.constants.SentimentAnalysisConstants;
import flink.parsers.CommonLogParser;
import flink.parsers.JsonTweetParser;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class LogProcessing extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(LogProcessing.class);
    private int volumeCountThreads;
    private int statusCountThreads;
    private int geoFinderThreads;
    private int geoStatsThreads;

    public LogProcessing(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        volumeCountThreads = config.getInteger(LogProcessingConstants.Conf.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInteger(LogProcessingConstants.Conf.STATUS_COUNTER_THREADS, 1);
        geoFinderThreads   = config.getInteger(LogProcessingConstants.Conf.GEO_FINDER_THREADS, 1);
        geoStatsThreads    = config.getInteger(LogProcessingConstants.Conf.GEO_STATS_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple7<Object, Object, Long, Object, Object, Object, String>> dataParse = data.map(new CommonLogParser());

        // Process
        DataStream<Tuple3<Long, Long, String>> volCount = dataParse.keyBy(value -> value.f2).filter(value -> (value != null)).flatMap(new VolumeCount(config)).setParallelism(volumeCountThreads);

        DataStream<Tuple3<Integer, Integer, String>> statusCount = dataParse.keyBy(value -> value.f3).filter(value -> (value != null)).flatMap(new StatusCount(config)).setParallelism(statusCountThreads);

        DataStream<Tuple3<String, String, String>> geoFind = dataParse.filter(value -> (value != null)).flatMap(new GeoFinder(config)).setParallelism(geoFinderThreads);

        DataStream<Tuple5<String, Integer, String, Integer, String>> geoStats = geoFind.keyBy(value -> value.f0).filter(value -> (value != null)).flatMap(new GeoStats(config)).setParallelism(geoStatsThreads);

        // Sink
        createSink(volCount);
        createSink(statusCount);
        createSink(geoStats);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return LogProcessingConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
