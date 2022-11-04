package flink.parsers;

import flink.util.Metrics;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;

/**
 *
 */
public abstract class Parser extends Metrics {
    protected Configuration config;

    public void initialize(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    public abstract Tuple1<?> parse(String input);
}