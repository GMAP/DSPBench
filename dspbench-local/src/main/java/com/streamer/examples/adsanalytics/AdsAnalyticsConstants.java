package com.streamer.examples.adsanalytics;

import com.streamer.base.constants.BaseConstants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface AdsAnalyticsConstants extends BaseConstants {
    String PREFIX = "aa";
    
    interface Config extends BaseConfig {
        String CTR_THREADS          = "aa.ctr.threads";
        String CTR_EMIT_FREQUENCY   = "aa.ctr.emit_frequency";
        String CTR_WINDOW_LENGTH    = "aa.ctr.window_length";
    }
    
    interface Component extends BaseComponent {
        String CTR = "ctrOperator";
        String CTR_AGGREGATOR = "aggregatorCtrOperator";
    }
    
    interface Streams {
        String CLICKS = "clickStream";
        String IMPRESSIONS = "impressionStream";
        String CTRS = "ctrStream";
    }
    
    interface Field {
        String QUERY_ID      = "queryId";
        String AD_ID         = "adId";
        String EVENT         = "event";
        String CTR           = "ctr";
        String IMPRESSIONS   = "impressions";
        String CLICKS        = "clicks";
        String WINDOW_LENGTH = "actualWindowLengthInSeconds";
    }
}