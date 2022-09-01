package org.dspbench.sink;

import org.apache.storm.tuple.Tuple;
import org.dspbench.applications.wordcount.WordCountConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 *
 * @author mayconbordin
 */
public class ConsoleSink extends BaseSink {
    private static final Logger LOG = LoggerFactory.getLogger(ConsoleSink.class);
    
    @Override
    public void execute(Tuple input) {
        System.out.println(formatter.format(input));
        collector.ack(input);
        super.calculateLatency(Long.parseLong(input.getStringByField(WordCountConstants.Field.INITTIME)));
        super.calculateThroughput();
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
}
