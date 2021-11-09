package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface SentimentAnalysisConstants extends BaseConstants {
    String PREFIX = "sa";
    
    interface Config extends BaseConfig {
        String PARSER_THREADS         = "sa.parser.threads";
        String FILTER_THREADS         = "sa.filter.threads";
        String STEMMER_THREADS        = "sa.stemmer.threads";
        String POS_SCORER_THREADS     = "sa.pos_scorer.threads";
        String NEG_SCORER_THREADS     = "sa.neg_scorer.threads";
        String SCORER_THREADS         = "sa.scorer.threads";
        String JOINER_THREADS         = "sa.joiner.threads";
    }
}
