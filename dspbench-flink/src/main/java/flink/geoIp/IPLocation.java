package flink.geoIp;

/**
 *
 */
public interface IPLocation {
    public Location resolve(String ip);
}
