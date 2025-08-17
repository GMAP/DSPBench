package flink.constants;

public interface BargainIndexConstants extends BaseConstants {
    String PREFIX = "bi";

    interface Conf extends BaseConf {
        String QUOTES_SOURCE_THREADS = "bi.quotes.source.threads";
        String QUOTES_PARSER_THREADS = "bi.quotes.parser.threads";
        String TRADES_SOURCE_THREADS = "bi.trades.source.threads";
        String TRADES_PARSER_THREADS = "bi.trades.parser.threads";

        String VWAP_THREADS = "bi.vwap.threads";
        String VWAP_PERIOD  = "bi.vwap.period";
        String BARGAIN_INDEX_THREADS = "bi.bargainindex.threads";
        String BARGAIN_INDEX_THRESHOLD = "bi.bargainindex.threshold";
        String SINK_THREADS = "bi.sink.threads";
    }
}
