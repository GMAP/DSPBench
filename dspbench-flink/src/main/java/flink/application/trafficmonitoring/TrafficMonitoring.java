package flink.application.trafficmonitoring;

import flink.application.AbstractApplication;
import flink.application.sentimentanalysis.SentimentCalculator;
import flink.constants.SentimentAnalysisConstants;
import flink.constants.TrafficMonitoringConstants;
import flink.parsers.BeijingTaxiParser;
import flink.parsers.JsonTweetParser;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TrafficMonitoring extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private int mapMatcherThreads;
    private int speedCalcThreads;

    public TrafficMonitoring(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        mapMatcherThreads = config.getInteger(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads  = config.getInteger(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple8<String, DateTime, Boolean,Integer, Integer, Double, Double, String>> dataParse = data.map(new BeijingTaxiParser());

        // Process
        DataStream<Tuple9<String, DateTime, Boolean,Integer, Integer, Double, Double, Integer, String>> mapMatch = dataParse.filter(value -> (value != null)).flatMap(new MapMatching(config)).setParallelism(mapMatcherThreads);

        DataStream<Tuple5<Date, Integer, Integer, Integer, String>> speedCalc = mapMatch.keyBy(value -> value.f7).filter(value -> (value != null)).flatMap(new SpeedCalculator(config)).setParallelism(speedCalcThreads);

        // Sink
        createSink(speedCalc);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return TrafficMonitoringConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
