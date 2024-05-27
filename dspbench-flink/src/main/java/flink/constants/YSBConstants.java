package flink.constants;

public interface YSBConstants extends BaseConstants {
    String PREFIX = "ysb";

    interface Conf extends BaseConf {
        String NUM_CAMPAIGNS = "ysb.numKeys";

        String SOURCE_THREADS = "ysb.source.threads";
        String FILTER_THREADS = "ysb.filter.threads";
        String JOINER_THREADS = "ysb.joiner.threads";
        String AGGREGATOR_THREADS = "ysb.aggregator.threads";
        String SINK_THREADS = "ysb.sink.threads";
    }

    interface Component extends BaseComponent {
        String FILTER = "filter";
        String JOINER = "joiner";
        String AGGREGATOR = "aggregator";
    }
}
