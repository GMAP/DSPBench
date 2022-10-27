package flink.parsers;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class Parser{
    protected Configuration config;

    public void initialize(Configuration config) {
        this.config = config;
    }

    public abstract Tuple1<?> parse(String input);
}