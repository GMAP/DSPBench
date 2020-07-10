package com.streamer.metrics.reporter.statsd;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.Charset;

/**
 *
 * @author mayconbordin
 */
public class TCPSocket implements ISocket {
    private Socket socket;
    private PrintWriter out;

    public void connect(InetSocketAddress address) throws IOException {
        connect(address, null);
    }

    public void connect(InetSocketAddress address, Charset charset) throws IOException {
        this.socket = new Socket(address.getAddress(), address.getPort());
        this.out = new PrintWriter(socket.getOutputStream(), true);
    }

    public void close() throws IOException {
        out.close();
        socket.close();
    }

    public void send(String message) throws IOException {
        out.println(message);
    }
    
}
