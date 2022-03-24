package Constants;

/**
 *  @author  Alessandra Fais
 *  @version July 2019
 *
 *  Constants set for all applications.
 */
public interface BaseConstants {
    String HELP = "help";

    interface Execution {
        String LOCAL_MODE = "local";
        String REMOTE_MODE = "remote";
        int DEFAULT_RATE = -1;
        int RUNTIME_SEC = 60;  // topology is alive for 60 seconds (valid for Storm)
    }

    interface BaseComponent {
        String SPOUT = "spout";
        String SINK  = "sink";
    }

    interface BaseField {
        String TIMESTAMP = "timestamp";
    }
}
