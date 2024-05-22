package flink.constants;

public interface AdAnalyticsConstants extends BaseConstants {
    String PREFIX = "aa";

    interface Conf extends BaseConf {
        String CLICKS_SOURCE_THREADS = "aa.click.source.threads";
        String CLICKS_PARSER_THREADS = "aa.click.parser.threads";
        String IMPRESSIONS_SOURCE_THREADS = "aa.impressions.source.threads";
        String IMPRESSIONS_PARSER_THREADS = "aa.impressions.parser.threads";
        String CTR_THREADS = "aa.ctr.threads";
        String CTR_EMIT_FREQUENCY = "aa.ctr.emit_frequency";
        String CTR_WINDOW_LENGTH = "aa.ctr.window_length";
        String SINK_THREADS = "aa.sink.threads";
    }
}
