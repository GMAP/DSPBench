package flink.constants;

public interface SentimentAnalysisConstants extends BaseConstants {
    String PREFIX = "sa";

    interface Conf extends BaseConf {
        String SOURCE_THREADS = "sa.source.threads";
        String PARSER_THREADS = "sa.parser.threads";

        String CLASSIFIER_THREADS = "sa.classifier.threads";
        String CLASSIFIER_TYPE = "sa.classifier.type";
        String LINGPIPE_CLASSIFIER_PATH = "sa.classifier.lingpipe.path";
        String BASIC_CLASSIFIER_PATH = "sa.classifier.basic.path";
        String SINK_THREADS = "sa.sink.threads";
    }

    interface Component extends BaseComponent {
        String CLASSIFIER = "classifierBolt";
    }
}
