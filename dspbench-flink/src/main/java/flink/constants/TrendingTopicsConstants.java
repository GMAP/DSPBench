package flink.constants;

public interface TrendingTopicsConstants extends BaseConstants {
    String PREFIX = "tt";

    interface Conf extends BaseConf {
        String SOURCE_THREADS = "tt.source.threads";
        String PARSER_THREADS = "tt.parser.threads";

        String TOPIC_EXTRACTOR_THREADS = "tt.topic_extractor.threads";
        String COUNTER_THREADS = "tt.counter.threads";
        String COUNTER_WINDOW = "tt.counter.window_length";
        String COUNTER_FREQ = "tt.counter.frequency";
        String IRANKER_THREADS = "tt.iranker.threads";
        String IRANKER_FREQ = "tt.iranker.frequency";
        String TRANKER_THREADS = "tt.tranker.threads";
        String TRANKER_FREQ = "tt.tranker.frequency";
        String TOPK = "tt.topk";
        
        String SINK_THREADS = "tt.sink.threads";
    }
}
