package com.streamer.examples.voipstream;


import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Component;
import com.streamer.core.Operator;
import com.streamer.examples.utils.bloom.ODTDBloomFilter;
import com.streamer.utils.Configuration;
import org.apache.log4j.Logger;
import com.streamer.examples.voipstream.VoIPSTREAMConstants.*;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractFilterOperator extends Operator {
    private static final Logger LOG = Logger.getLogger(AbstractFilterOperator.class);
    
    protected ODTDBloomFilter filter;

    protected String configPrefix;

    public AbstractFilterOperator(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    @Override
    public void onCreate(int id, Configuration config) {
        super.onCreate(id, config);

        int numElements       = config.getInt(String.format(Config.FILTER_NUM_ELEMENTS, configPrefix));
        int bucketsPerElement = config.getInt(String.format(Config.FILTER_BUCKETS_PEL, configPrefix));
        int bucketsPerWord    = config.getInt(String.format(Config.FILTER_BUCKETS_PWR, configPrefix));
        double beta           = config.getDouble(String.format(Config.FILTER_BETA, configPrefix));

        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }

    @Override
    public Component copy() {
        Component newInstance = super.copy();
        ((AbstractFilterOperator) newInstance).setConfigPrefix(configPrefix);

        return newInstance;
    }
}