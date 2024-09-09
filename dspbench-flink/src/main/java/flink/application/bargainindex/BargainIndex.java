package flink.application.bargainindex;

import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.AbstractApplication;
import flink.constants.BargainIndexConstants;
import flink.parsers.StockQuotesParser;

public class BargainIndex extends AbstractApplication{

    private static final Logger LOG = LoggerFactory.getLogger(BargainIndex.class);

    private int quotesParserThreads;
    private int tradesParserThreads;
    private int VWAPThreads;
    private int bargainingIndexThreads;

    public BargainIndex(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        quotesParserThreads = config.getInteger(BargainIndexConstants.Conf.QUOTES_PARSER_THREADS, 1);
        tradesParserThreads = config.getInteger(BargainIndexConstants.Conf.TRADES_PARSER_THREADS, 1);
        VWAPThreads = config.getInteger(BargainIndexConstants.Conf.VWAP_THREADS, 1);
        bargainingIndexThreads = config.getInteger(BargainIndexConstants.Conf.BARGAIN_INDEX_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> quotes = createSource("quotes");
        DataStream<String> trades = createSource("trades");

        // Parser
        DataStream<Tuple5<String, Double, Integer, DateTime, Integer>> quotesParser = quotes.map(new StockQuotesParser(config, "quotes")).filter(value -> (value != null)).setParallelism(quotesParserThreads);
        DataStream<Tuple5<String, Double, Integer, DateTime, Integer>> tradesParser = trades.map(new StockQuotesParser(config, "trades")).filter(value -> (value != null)).setParallelism(tradesParserThreads);

        // Process
        DataStream<Tuple4<String, Double, DateTime, DateTime>> VWAP = tradesParser.keyBy(value -> value.f0).flatMap(new VWAP(config)).setParallelism(VWAPThreads);

        DataStream<Tuple4<String, Double, Integer, Double>> bargainIdx = VWAP.keyBy(value -> value.f0)
        .connect(quotesParser.keyBy(value -> value.f0))
        .flatMap(new BargainingIndex(config)).setParallelism(bargainingIndexThreads);

        // Sink
        createSinkBI(bargainIdx);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return BargainIndexConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
