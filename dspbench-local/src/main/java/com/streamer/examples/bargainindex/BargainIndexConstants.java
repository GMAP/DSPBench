package com.streamer.examples.bargainindex;

import com.streamer.base.constants.BaseConstants;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface BargainIndexConstants extends BaseConstants {
    String PREFIX = "bi";
    
    interface Config extends BaseConfig {
        String VWAP_THREADS = "bi.vwap.threads";
        String VWAP_PERIOD  = "bi.vwap.period";
        String BARGAIN_INDEX_THREADS = "bi.bargainindex.threads";
        String BARGAIN_INDEX_THRESHOLD = "bi.bargainindex.threshold";
    }
    
    interface Component extends BaseComponent {
        String VWAP = "vwapOperator";
        String BARGAIN_INDEX = "bargainIndexOperator";
    }
    
    interface Periodicity {
        String MINUTELY = "minutely";
        String HOURLY   = "hourly";
        String DAILY    = "daily";
        String WEEKLY   = "weekly";
        String MONTHLY  = "monthly";
    }
    
    interface Streams {
        String QUOTES   = "quoteStream";
        String TRADES   = "tradeStream";
        String VWAP     = "vwapStream";
        String BARGAINS = "bargainStream";
    }
    
    interface Field {
        String STOCK = "stock";
        String PRICE = "price";
        String VOLUME = "volume";
        String DATE   = "date";
        String START_DATE = "startDate";
        String END_DATE   = "endDate";
        String INTERVAL = "interval";
        String VWAP = "vwap";
        String BARGAIN_INDEX = "bargainIndex";
    }
}
