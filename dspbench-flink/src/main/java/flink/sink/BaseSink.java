package flink.sink;

import flink.constants.BaseConstants;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public abstract class BaseSink{
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);

    private final Map<String, Long> throughput = new HashMap<>();
    private final ArrayList<Long> latency = new ArrayList<Long>();
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }
    protected abstract Logger getLogger();

    public void sinkStream(DataStream<?> dt) {}
}
