package org.dspbench.applications.voipstream;


import org.dspbench.core.Component;
import org.dspbench.core.Operator;
import org.dspbench.utils.bloom.ODTDBloomFilter;
import org.dspbench.utils.Configuration;
import org.dspbench.applications.voipstream.VoIPSTREAMConstants.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractFilterOperator extends Operator {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFilterOperator.class);
    
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