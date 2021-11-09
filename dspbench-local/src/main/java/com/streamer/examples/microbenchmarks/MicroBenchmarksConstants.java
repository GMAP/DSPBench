package com.streamer.examples.microbenchmarks;

import com.streamer.base.constants.BaseConstants;

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
