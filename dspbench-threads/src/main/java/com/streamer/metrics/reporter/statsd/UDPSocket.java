package com.streamer.metrics.reporter.statsd;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class UDPSocket implements ISocket {
    private static final Logger LOG = LoggerFactory.getLogger(UDPSocket.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    private DatagramSocket socket;
    private InetSocketAddress address;
    private Charset charset;
    
    public void connect(InetSocketAddress address, Charset charset) throws IOException {
        this.address = address;
        this.charset = charset;
        this.socket = new DatagramSocket();
    }

    public void connect(InetSocketAddress address) throws IOException {
        this.connect(address, UTF_8);
    }

    public void close() throws IOException {
        socket.close();
    }

    public void send(String message) throws IOException {
        byte[] bytes = message.getBytes(charset);
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, address);
        socket.send(packet);
    }
    
}
