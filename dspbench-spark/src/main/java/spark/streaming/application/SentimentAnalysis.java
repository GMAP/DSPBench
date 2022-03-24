package spark.streaming.application;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import static spark.streaming.constants.SentimentAnalysisConstants.*;
import spark.streaming.function.SentimentScorer;
import spark.streaming.function.SentimentTypeScorer;
import spark.streaming.function.SentimentTypeScorer.Type;
import spark.streaming.sink.Sink;
import spark.streaming.function.Stemmer;
import spark.streaming.function.TweetFilter;
import spark.streaming.function.TweetJoiner;
import spark.streaming.function.TweetParser;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class SentimentAnalysis extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysis.class);
    
    private int batchSize;
    private int parserThreads;
    private int filterThreads;
    private int stemmerThreads;
    private int posScorerThreads;
    private int negScorerThreads;
    private int scorerThreads;
    private int joinerThreads;
    
    public SentimentAnalysis(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        batchSize        = config.getInt(getConfigKey(Config.BATCH_SIZE), 1000);
        parserThreads    = config.getInt(Config.PARSER_THREADS, 1);
        filterThreads    = config.getInt(Config.FILTER_THREADS, 1);
        stemmerThreads   = config.getInt(Config.STEMMER_THREADS, 1);
        posScorerThreads = config.getInt(Config.POS_SCORER_THREADS, 1);
        negScorerThreads = config.getInt(Config.NEG_SCORER_THREADS, 1);
        scorerThreads    = config.getInt(Config.SCORER_THREADS, 1);
        joinerThreads    = config.getInt(Config.JOINER_THREADS, 1);
    }

    @Override
    public JavaStreamingContext buildApplication() {
        context = new JavaStreamingContext(config, new Duration(batchSize));
                
        JavaDStream<Tuple2<String, Tuple>> rawTweets = createSource();
        
        JavaDStream<Tuple2<Long, Tuple>> tweets = rawTweets.repartition(parserThreads)
                .map(new TweetParser(config));
        
        JavaDStream<Tuple2<Long, Tuple>> tweetsFiltered = tweets.repartition(filterThreads)
                .filter(new TweetFilter(config));
        
        tweetsFiltered = tweetsFiltered.repartition(stemmerThreads)
                .map(new Stemmer(config));
        
        JavaPairDStream<Long, Tuple2<Tuple, Float>> positiveTweets = tweetsFiltered.repartition(posScorerThreads)
                .mapToPair(new SentimentTypeScorer(Type.Positive, config, "positiveScorer"));
        
        JavaPairDStream<Long, Tuple2<Tuple, Float>> negativeTweets = tweetsFiltered.repartition(negScorerThreads)
                .mapToPair(new SentimentTypeScorer(Type.Negative, config, "negativeScorer"));

        JavaPairDStream<Long, Tuple2<Tuple2<Tuple, Float>, Tuple2<Tuple, Float>>> joined = positiveTweets.join(negativeTweets, joinerThreads);
        
        JavaDStream<Tuple2<Long, Tuple>> scoredTweets = joined.repartition(joinerThreads)
                .map(new TweetJoiner(config));
        
        JavaDStream<Tuple2<Long, Tuple>> results = scoredTweets.repartition(scorerThreads)
                .map(new SentimentScorer(config));
        
        results.foreachRDD(new Sink<Long>(config));
        
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
