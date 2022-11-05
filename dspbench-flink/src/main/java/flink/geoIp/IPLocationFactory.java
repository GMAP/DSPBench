package flink.geoIp;

import flink.constants.BaseConstants;
import org.apache.flink.configuration.Configuration;

/**
 *
 */
public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";
    
    public static IPLocation create(String name, Configuration config) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location(config.getString(BaseConstants.BaseConf.GEOIP2_DB, ""));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
