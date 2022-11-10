package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.*;
import spark.streaming.util.Configuration;

public class WordCount extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    private int parserThreads;
    private int splitterThreads;
    private int singleCounterThreads;
    private int pairCounterThreads;

    public WordCount(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        splitterThreads = config.getInt(WordCountConstants.Config.SPLITTER_THREADS, 1);
        singleCounterThreads = config.getInt(WordCountConstants.Config.SINGLE_COUNTER_THREADS, 1);
        pairCounterThreads = config.getInt(WordCountConstants.Config.PAIR_COUNTER_THREADS, 1);
        parserThreads = config.getInt(WordCountConstants.Config.PARSER_THREADS, 1);
    }

    @Override
    public DataStreamWriter<Row> buildApplication() {

        Dataset<Row> lines = createSource();

        Dataset<Row> records = lines.repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSWordcountParser(config), Encoders.kryo(Row.class));

        Dataset<Row> words = records.repartition(splitterThreads)
                .filter(new SSFilterNull<>())
                .flatMap(new Split(config),  Encoders.kryo(Row.class));

        Dataset<Row> wordCounts = words
                .repartition(pairCounterThreads)
                .groupByKey((MapFunction<Row, String>) row -> row.getString(0), Encoders.STRING())
                .mapGroupsWithState(new SSWordCount(config), Encoders.LONG(), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

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
