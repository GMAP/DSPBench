package LogProcessing.geoip;

/**
 *
 * @author mayconbordin
 */
public class IPLocationFactory {
    public static final String GEOIP2 = "geoip2";
    
    public static IPLocation create(String name) {
        if (name.equals(GEOIP2)) {
            return new GeoIP2Location("/home/gabriel/Documents/repos/DSPBenchLarcc/dspbench-flink/data/GeoLite2-City.mmdb");
        } else {
            throw new IllegalArgumentException(name + " is not a valid IP locator name");
        }
    }
}
