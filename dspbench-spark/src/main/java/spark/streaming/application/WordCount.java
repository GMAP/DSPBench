package spark.streaming.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.Split;
import spark.streaming.util.Configuration;

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
        batchSize = config.getInt(getConfigKey(WordCountConstants.Config.BATCH_SIZE), 1000);
        splitterThreads = config.getInt(WordCountConstants.Config.SPLITTER_THREADS, 1);
        singleCounterThreads = config.getInt(WordCountConstants.Config.SINGLE_COUNTER_THREADS, 1);
        pairCounterThreads = config.getInt(WordCountConstants.Config.PAIR_COUNTER_THREADS, 1);
    }

    @Override
    public DataStreamWriter<Row> buildApplication() {

        Dataset<Row> lines = createSource();

        Dataset<String> words = lines.repartition(splitterThreads)
                .as(Encoders.STRING())
                .flatMap(new Split(config), Encoders.STRING());

        Dataset<Row> wordCounts = words.repartition(pairCounterThreads).groupBy("value").count();

        return createSink(wordCounts);
    }

    @Override
    public String getConfigPrefix() {
        return WordCountConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
