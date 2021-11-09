package spark.streaming.constants;

/**
 *
 * @author mayconbordin
 */
public interface WordCountConstants extends BaseConstants {
    String PREFIX = "wc";
    
    interface Config extends BaseConfig {
        String SPLITTER_THREADS       = "wc.splitter.threads";
        String SINGLE_COUNTER_THREADS = "wc.single_counter.threads";
        String PAIR_COUNTER_THREADS   = "wc.pair_counter.threads";
    }
}
