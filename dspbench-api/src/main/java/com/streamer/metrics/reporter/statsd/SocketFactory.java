package com.streamer.metrics.reporter.statsd;

/**
 *
 * @author mayconbordin
 */
public class SocketFactory {
    public static final String TCP = "tcp";
    public static final String UDP = "udp";
    
    public static ISocket newInstance(String protocol) {
        if (protocol.equals(TCP)) {
            return new TCPSocket();
        } else if (protocol.equals(UDP)) {
            return new UDPSocket();
        } else {
            throw new IllegalArgumentException("Protocol "+protocol+" is not supported");
        }
    }
}
