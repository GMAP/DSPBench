package Constants;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  Constants peculiar of the WordCount application.
 */
public interface WordCountConstants extends BaseConstants {
    String DEFAULT_PROPERTIES = "/wordcount/wc.properties";
    String DEFAULT_TOPO_NAME = "WordCount";

    interface Conf {
        String FILE_SOURCE = "file";
        String GEN_SOURCE = "generator";
        String SPOUT_PATH = "wc.spout.path";
        String SPOUT_THREADS = "wc.spout.threads";
        String SPLITTER_THREADS = "wc.splitter.threads";
        String COUNTER_THREADS = "wc.counter.threads";
        String SINK_THREADS = "wc.sink.threads";
        String ALL_THREADS = "wc.all.threads"; // useful only with Flink
    }

    interface Component extends BaseComponent {
        String SPLITTER = "splitter";
        String COUNTER = "counter";
    }

    interface Field extends BaseField {
        String LINE = "line";
        String WORD = "word";
        String COUNT = "count";
    }
}