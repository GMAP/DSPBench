package org.dspbench.applications.spikedetection;

import org.dspbench.base.source.generator.Generator;
import org.dspbench.core.Values;
import org.dspbench.applications.spikedetection.SpikeDetectionConstants.Config;
import org.dspbench.utils.Configuration;
import java.util.Date;
import java.util.Random;
import org.apache.commons.lang3.RandomStringUtils;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class SensorGenerator extends Generator {
    private long count;  
    private String deviceID;
    private final Random random = new Random();
    
    @Override
    public void initialize(Configuration config) {
        super.initialize(config);
        
        count = config.getLong(Config.GENERATOR_COUNT, 1000000);
        deviceID = RandomStringUtils.randomAlphanumeric(20);
    }

    @Override
    public Values generate() {
        Values values = null;
        if (count-- > 0) {
            values = new Values(deviceID, new Date(), (random.nextDouble() * 10) + 50);                        
        } else if (count-- == -1) {
            values = new Values(deviceID, -1.0);
        }
        
        if (values != null) {
            values.setId(String.format("%s:%s", deviceID, count).hashCode());
        }
        
        return values;
    }
    
}