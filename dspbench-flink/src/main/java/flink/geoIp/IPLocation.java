package flink.geoIp;

/**
 *
 */
public interface IPLocation {
    Location resolve(String ip);
}
