package flink.application.wordcount;

import flink.application.AbstractApplication;
import flink.constants.WordCountConstants;
import flink.parsers.StringParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCount extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    private int splitSentenceThreads;
    private int wordCountThreads;

    public WordCount(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        splitSentenceThreads = config.getInteger(WordCountConstants.Conf.SPLITTER_THREADS, 1);
        wordCountThreads     = config.getInteger(WordCountConstants.Conf.COUNTER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple2<String, String>> dataParse = data.map(new StringParser(config));

        // Process
        DataStream<Tuple3<String, Integer, String>> splitter = dataParse.filter(value -> (value != null)).flatMap(new Splitter(config)).setParallelism(splitSentenceThreads);

        DataStream<Tuple3<String, Integer, String>> count = splitter.keyBy(value -> value.f0).flatMap(new Counter(config)).setParallelism(wordCountThreads);

        // Sink
        createSink(count);

        return env;
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
