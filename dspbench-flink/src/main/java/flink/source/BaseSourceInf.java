package flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * @author gabrielfim
 */
public abstract class BaseSourceInf{
    protected Configuration config;
    protected transient StreamExecutionEnvironment env;
    protected String prefix;

    public void initialize(Configuration config, StreamExecutionEnvironment env, String prefix) {
        this.config = config;
        this.env = env;
        this.prefix = prefix;
    }
    
    public abstract String createStream() throws Exception;
}
