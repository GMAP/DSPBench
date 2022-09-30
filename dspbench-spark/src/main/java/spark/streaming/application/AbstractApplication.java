package spark.streaming.application;

import java.io.Serializable;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.sink.BaseSink;
import spark.streaming.source.BaseSource;
import spark.streaming.util.ClassLoaderUtils;
import spark.streaming.util.Configuration;

/**
 *
 * @author mayconbordin
 */
public abstract class AbstractApplication implements Serializable {
    protected String appName;
    protected Configuration config;
    protected transient SparkSession session;
    public AbstractApplication(String appName, Configuration config) {
        this.appName = appName;
        this.config = config;
        this.session = SparkSession
                .builder()
                .config(config)
                .getOrCreate();

        this.session.sparkContext().setLogLevel("WARN");
    }
    
    public abstract void initialize();
    public abstract DataStreamWriter buildApplication();
    public abstract String getConfigPrefix();
    public abstract Logger getLogger();

    public String getAppName() {
        return appName;
    }
    
    protected Dataset<Row> createSource() {
        String sourceClass = config.get(getConfigKey(BaseConfig.SOURCE_CLASS));
        BaseSource source = (BaseSource) ClassLoaderUtils.newInstance(sourceClass, "source", getLogger());
        source.initialize(config, session, getConfigPrefix());
        return source.createStream();
    }
    protected DataStreamWriter<Row> createSink(Dataset<Row> dt) {
        String sinkClass = config.get(getConfigKey(BaseConfig.SINK_CLASS));
        BaseSink source = (BaseSink) ClassLoaderUtils.newInstance(sinkClass, "sink", getLogger());
        source.initialize(config, session);
        return source.sinkStream(dt);
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
