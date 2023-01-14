package flink.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 * @author gabrielfim
 */
public abstract class BaseSource{
    protected Configuration config;
    protected transient StreamExecutionEnvironment env;
    protected String prefix;

    public void initialize(Configuration config, StreamExecutionEnvironment env, String prefix) {
        this.config = config;
        this.env = env;
        this.prefix = prefix;
    }
    
    public abstract DataStream<String> createStream();
}
