package flink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);

    @Override
    public void sinkStream(DataStream<?> input) {
        input.print();
        //super.calculateLatency(Long.parseLong(input.));
        super.calculateThroughput();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
}
