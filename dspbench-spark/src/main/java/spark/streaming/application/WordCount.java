package spark.streaming.application;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.constants.WordCountConstants.Config;
import static spark.streaming.constants.WordCountConstants.*;
import spark.streaming.function.CountSingleWords;
import spark.streaming.function.CountWordPairs;
import spark.streaming.sink.PairSink;
import spark.streaming.function.Split;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class WordCount extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);
    
    private int batchSize;
    private int splitterThreads;
    private int singleCounterThreads;
    private int pairCounterThreads;
    
    public WordCount(String appName, Configuration config) {
        super(appName, config);
    }
    
    @Override
    public void initialize() {
        batchSize            = config.getInt(getConfigKey(Config.BATCH_SIZE), 1000);
        splitterThreads      = config.getInt(Config.SPLITTER_THREADS, 1);
        singleCounterThreads = config.getInt(Config.SINGLE_COUNTER_THREADS, 1);
        pairCounterThreads   = config.getInt(Config.PAIR_COUNTER_THREADS, 1);
    }

    @Override
    public JavaStreamingContext buildApplication() {
        context = new JavaStreamingContext(config, new Duration(batchSize));

        JavaDStream<Tuple2<String, Tuple>> lines = createSource();

        JavaDStream<Tuple2<String, Tuple>> words = lines.repartition(splitterThreads)
                .flatMap(new Split(config));
                
        JavaPairDStream<String, Tuple> singleCounts = words.repartition(singleCounterThreads)
                .mapToPair(new CountSingleWords(config));
                
        JavaPairDStream<String, Tuple> wordCounts = singleCounts.reduceByKey(
                new CountWordPairs(config), pairCounterThreads);
        
        wordCounts.foreachRDD(new PairSink<String>(config));
                
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
