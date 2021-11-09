package spark.streaming.function;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import java.io.Serializable;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.util.Configuration;
import spark.streaming.metrics.MetricsFactory;

/**
 *
 * @author mayconbordin
 */
public abstract class BaseFunction implements Serializable {    
    private String name;
    private Integer id;
    private transient MetricRegistry metrics;
    private transient Counter tuplesReceived;
    private transient Counter tuplesEmitted;
    private transient Configuration config;
    private String configStr;
    
    public BaseFunction(Configuration config) {
        this();
        this.configStr = config.toString();
    }
    
    public BaseFunction(Configuration config, String name) {
        this.name = name;
        this.configStr = config.toString();
    }

    public BaseFunction(String name) {
        this.name = name;
    }
    
    public BaseFunction() {
        this.name = this.getClass().getSimpleName();
    }
    
    public void setConfiguration(String cfg) {
        configStr = cfg;
    }
    
    public Configuration getConfiguration() {
        if (config == null) {
            config = Configuration.fromStr(configStr);
        }
        
        return config;
    }
    
    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(getConfiguration());
        }
        return metrics;
    }
    
    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(String.format("%s-%d.tuples-received", name, getId()));
        }
        return tuplesReceived;
    }
    
    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(String.format("%s-%d.tuples-emitted", name, getId()));
        }
        return tuplesEmitted;
    }
    
    protected void incReceived() {
        getTuplesReceived().inc();
    }
    
    protected void incReceived(long n) {
        getTuplesReceived().inc(n);
    }
    
    protected void incEmitted() {
        getTuplesEmitted().inc();
    }
    
    protected void incEmitted(long n) {
        getTuplesEmitted().inc(n);
    }
    
    protected void incBoth() {
        getTuplesReceived().inc();
        getTuplesEmitted().inc();
    }
    
    protected int getId() {
        if (id == null) {
            id = Math.abs(UUID.randomUUID().hashCode());
        }
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
