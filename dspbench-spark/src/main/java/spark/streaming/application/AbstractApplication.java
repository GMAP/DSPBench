package spark.streaming.application;

import java.io.Serializable;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.source.BaseSource;
import spark.streaming.util.ClassLoaderUtils;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public abstract class AbstractApplication implements Serializable {
    protected String appName;
    protected Configuration config;
    
    protected transient JavaStreamingContext context;

    public AbstractApplication(String appName, Configuration config) {
        this.appName = appName;
        this.config = config;
    }
    
    public abstract void initialize();
    public abstract JavaStreamingContext buildApplication();
    public abstract String getConfigPrefix();
    public abstract Logger getLogger();

    public String getAppName() {
        return appName;
    }
    
    protected JavaDStream<Tuple2<String, Tuple>> createSource() {
        String sourceClass = config.get(getConfigKey(BaseConfig.SOURCE_CLASS));
        BaseSource source = (BaseSource) ClassLoaderUtils.newInstance(sourceClass, "source", getLogger());
        source.initialize(config, context, getConfigPrefix());
        return source.createStream();
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
