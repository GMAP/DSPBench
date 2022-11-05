package spark.streaming.util.geoip;

import spark.streaming.util.Configuration;

import java.io.Serializable;


/**
 *
 * @author mayconbordin
 */
public class IPLocationFactory implements Serializable {
    public static final String GEOIP2 = "geoip2";
    
    public static IPLocation create(String name, Configuration config) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location(config.get(Configuration.GEOIP2_DB));
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
