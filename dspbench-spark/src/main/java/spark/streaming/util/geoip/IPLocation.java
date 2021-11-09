package spark.streaming.util.geoip;

/**
 *
 * @author mayconbordin
 */
public interface IPLocation {
    public Location resolve(String ip);
}
