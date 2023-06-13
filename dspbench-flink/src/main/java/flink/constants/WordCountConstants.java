package flink.constants;

public interface WordCountConstants extends BaseConstants {
    String PREFIX = "wc";

    interface Conf extends BaseConf {
        String SOURCE_THREADS = "wc.source.threads";
        String PARSER_THREADS = "wc.parser.threads";
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
        String SINK_THREADS = "wc.sink.threads";
    }

    interface Component extends BaseComponent {
        String SPLITTER = "splitSentence";
        String COUNTER = "wordCount";
    }
}
