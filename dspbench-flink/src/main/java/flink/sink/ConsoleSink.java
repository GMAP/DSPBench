package flink.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    @Override
    public void sinkStream(DataStream<?> input) {
        //System.out.println(input);
        input.print().name("WordCount-sink");
        //super.calculateLatency(Long.parseLong(input.getStringByField(WordCountConstants.Field.INITTIME)));
        //super.calculateThroughput();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
}
