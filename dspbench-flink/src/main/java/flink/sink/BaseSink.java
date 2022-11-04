package flink.sink;

import flink.constants.BaseConstants;
import flink.util.Metrics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public abstract class BaseSink extends Metrics {
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);

    private final Map<String, Long> throughput = new HashMap<>();
    private final ArrayList<Long> latency = new ArrayList<Long>();
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    Configuration config;

    public void initialize(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }
    protected abstract Logger getLogger();

    public void sinkStream(DataStream<?> dt) {}
}
