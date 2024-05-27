package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface SentimentAnalysisConstants extends BaseConstants {
    String PREFIX = "sa";

    interface Config extends BaseConfig {
        String PARSER_THREADS           = "sa.parser.threads";
        String CLASSIFIER_THREADS       = "sa.classifier.threads";
        String CLASSIFIER_TYPE          = "sa.classifier.type";
        String LINGPIPE_CLASSIFIER_PATH = "sa.classifier.lingpipe.path";
        String BASIC_CLASSIFIER_PATH    = "sa.classifier.basic.path";
    }
}
