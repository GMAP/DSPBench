package flink.application.logprocessing;

import flink.application.AbstractApplication;
import flink.constants.LogProcessingConstants;
import flink.parsers.CommonLogParser;
import flink.source.InfSourceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProcessing extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(LogProcessing.class);
    private int parserThreads;
    private int volumeCountThreads;
    private int statusCountThreads;
    private int geoFinderThreads;
    private int geoStatsThreads;

    public LogProcessing(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInteger(LogProcessingConstants.Conf.PARSER_THREADS, 1);
        volumeCountThreads = config.getInteger(LogProcessingConstants.Conf.VOLUME_COUNTER_THREADS, 1);
        statusCountThreads = config.getInteger(LogProcessingConstants.Conf.STATUS_COUNTER_THREADS, 1);
        geoFinderThreads = config.getInteger(LogProcessingConstants.Conf.GEO_FINDER_THREADS, 1);
        geoStatsThreads = config.getInteger(LogProcessingConstants.Conf.GEO_STATS_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple6<Object, Object, Long, Object, Object, Object>> dataParse = data
                .flatMap(new CommonLogParser(config)).setParallelism(parserThreads);

        // Process
        DataStream<Tuple2<Long, Long>> volCount = dataParse.filter(value -> (value.f2 != null)).keyBy(value -> value.f2)
                .flatMap(new VolumeCount(config)).setParallelism(volumeCountThreads);

        DataStream<Tuple2<Integer, Integer>> statusCount = dataParse.filter(value -> (value.f3 != null))
                .keyBy(value -> value.f3).flatMap(new StatusCount(config)).setParallelism(statusCountThreads);

        DataStream<Tuple2<String, String>> geoFind = dataParse.filter(value -> (value.f0 != null))
                .flatMap(new GeoFinder(config)).setParallelism(geoFinderThreads);

        DataStream<Tuple4<String, Integer, String, Integer>> geoStats = geoFind.filter(value -> (value.f0 != null))
                .keyBy(value -> value.f0).flatMap(new GeoStats(config)).setParallelism(geoStatsThreads);

        // Sink
        createSinkLPVol(volCount);
        createSinkLPStatus(statusCount);
        createSinkLPGeo(geoStats);

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
