package com.streamer.base.sink;

import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.core.Tuple;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class SampleFileSink extends FileSink {
    private static final Logger LOG = LoggerFactory.getLogger(SampleFileSink.class);
            
    private double sampleRate;
    private Random random;

    @Override
    public void initialize() {
        super.initialize();
        
        sampleRate = config.getDouble(getConfigKey(BaseConfig.SINK_SAMPLE_RATE), 0.05);
        random = new Random();
    }

    @Override
    public void process(Tuple tuple) {
        if (random.nextDouble() < sampleRate)
            super.process(tuple);
    }
    
    @Override
    protected Logger getLogger() {
        return LOG;
    }

}