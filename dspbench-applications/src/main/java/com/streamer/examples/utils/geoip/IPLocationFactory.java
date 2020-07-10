package com.streamer.examples.utils.geoip;

import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.utils.Configuration;

/**
 *
 * @author mayconbordin
 */
public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";
    
    public static IPLocation create(String name, Configuration config) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location(config.getString(BaseConfig.GEOIP2_DB));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
