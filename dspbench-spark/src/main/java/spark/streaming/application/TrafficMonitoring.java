package spark.streaming.application;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function2;
import scala.Tuple2;
import static spark.streaming.constants.TrafficMonitoringConstants.*;
import spark.streaming.constants.TrafficMonitoringConstants.Config;
import spark.streaming.function.BeijingTaxiTraceParser;
import spark.streaming.function.CountSingleWords;
import spark.streaming.function.CountWordPairs;
import spark.streaming.function.FilterNull;
import spark.streaming.function.MapMatcher;
import spark.streaming.function.SpeedCalculator;
import spark.streaming.function.Split;
import spark.streaming.sink.PairSink;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.List;
import java.util.Optional;

/**
 *
 * @author mayconbordin
 */
public class TrafficMonitoring extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    
    private String checkpointPath;
    private int batchSize;
    private int parserThreads;
    private int mapMatcherThreads;
    private int speedCalculatorThreads;
    
    public TrafficMonitoring(String appName, Configuration config) {
        super(appName, config);
    }
    
    @Override
    public void initialize() {
        checkpointPath         = config.get(getConfigKey(Config.CHECKPOINT_PATH), ".");
        batchSize              = config.getInt(getConfigKey(Config.BATCH_SIZE), 1000);
        parserThreads          = config.getInt(Config.PARSER_THREADS, 1);
        mapMatcherThreads      = config.getInt(Config.MAP_MATCHER_THREADS, 1);
        speedCalculatorThreads = config.getInt(Config.SPEED_CALCULATOR_THREADS, 1);
    }

    @Override
    public JavaStreamingContext buildApplication() {
        context = new JavaStreamingContext(config, new Duration(batchSize));
        context.checkpoint(checkpointPath);

        JavaDStream<Tuple2<String, Tuple>> rawRecords = createSource();
        
        JavaDStream<Tuple2<Integer, Tuple>> records = rawRecords.repartition(parserThreads)
                .map(new BeijingTaxiTraceParser(config));
        
        
        JavaPairDStream<Integer, Tuple> roads = records.filter(new FilterNull<Integer, Tuple>())
                .repartition(mapMatcherThreads).mapToPair(new MapMatcher(config));
                
        JavaPairDStream<Integer, Tuple> speeds = roads.filter(new FilterNull<Integer, Tuple>())
                .updateStateByKey(new SpeedCalculator(config), speedCalculatorThreads);

        speeds.foreachRDD(new PairSink<Integer>(config));
                
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
