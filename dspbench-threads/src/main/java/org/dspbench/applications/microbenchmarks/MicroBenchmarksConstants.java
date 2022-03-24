package org.dspbench.applications.microbenchmarks;

import org.dspbench.base.constants.BaseConstants;

/**
 *
 * @author mayconbordin
 */
public interface MicroBenchmarksConstants extends BaseConstants {
    String PREFIX = "mb";
    
    interface Field {
        String DATA  = "data";
    }
    
    interface Config extends BaseConfig {
        
    }
    
    interface Component extends BaseComponent {
        
    }
    
    interface Streams {
        String DATA = "dataStream";
    }
}
