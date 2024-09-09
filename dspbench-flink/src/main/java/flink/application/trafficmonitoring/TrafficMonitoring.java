package flink.application.trafficmonitoring;

import flink.application.AbstractApplication;
import flink.constants.TrafficMonitoringConstants;
import flink.parsers.BeijingTaxiParser;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class TrafficMonitoring extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private int parserThreads;
    private int mapMatcherThreads;
    private int speedCalcThreads;

    public TrafficMonitoring(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInteger(TrafficMonitoringConstants.Conf.PARSER_THREADS, 1);
        mapMatcherThreads = config.getInteger(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, 1);
        speedCalcThreads = config.getInteger(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple7<String, DateTime, Boolean, Integer, Integer, Double, Double>> dataParse = data.filter(value -> (value != null))
                .flatMap(new BeijingTaxiParser(config)).setParallelism(parserThreads);
        
        // Process
        DataStream<Tuple8<String, DateTime, Boolean, Integer, Integer, Double, Double, Integer>> mapMatch = dataParse
                .filter(value -> (value != null)).flatMap(new MapMatching(config)).setParallelism(mapMatcherThreads);
        
        DataStream<Tuple4<Date, Integer, Integer, Integer>> speedCalc = mapMatch.keyBy(value -> value.f7)
                .flatMap(new SpeedCalculator(config)).setParallelism(speedCalcThreads);

        // Sink
        createSinkTM(speedCalc);
        
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
