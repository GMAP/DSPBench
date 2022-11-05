package flink.application;

import java.io.Serializable;
import flink.constants.BaseConstants;
import flink.parsers.Parser;
import flink.sink.BaseSink;
import flink.source.BaseSource;
import flink.util.ClassLoaderUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;

/**
 *
 * @author gabrielfim
 */
public abstract class AbstractApplication implements Serializable {
    protected String appName;
    protected Configuration config;
    protected transient StreamExecutionEnvironment env;

    public AbstractApplication(String appName, Configuration config) {
        this.appName = appName;
        this.config = config;
        this.env = new StreamExecutionEnvironment();
    }
    
    public abstract void initialize();
    public abstract StreamExecutionEnvironment buildApplication();
        public abstract String getConfigPrefix();
    public abstract Logger getLogger();

    public String getAppName() {
        return appName;
    }

    protected DataStream<String> createSource() {
        String sourceClass = config.getString(getConfigKey(BaseConstants.BaseConf.SOURCE_CLASS),"flink.source.FileSource");
        BaseSource source = (BaseSource) ClassLoaderUtils.newInstance(sourceClass, "source", getLogger());
        source.initialize(config, env, getConfigPrefix());
        return source.createStream();
    }

    protected void createSink(DataStream<?> dt) {
        String sinkClass = config.getString(getConfigKey(BaseConstants.BaseConf.SINK_CLASS),"flink.sink.ConsoleSink");
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config);
        source.sinkStream(dt);
    }

    /**
     * Utility method to parse a configuration key with the application prefix and
     * component prefix.
     * @param key The configuration key to be parsed
     * @param name The name of the component
     * @return The formatted configuration key
     */
    protected String getConfigKey(String key, String name) {
        return String.format(key, String.format("%s.%s", getConfigPrefix(), name));
    }
    
    /**
     * Utility method to parse a configuration key with the application prefix..
     * @param key The configuration key to be parsed
     * @return
     */
    protected String getConfigKey(String key) {
        return String.format(key, getConfigPrefix());
    }
}
