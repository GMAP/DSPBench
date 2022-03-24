package com.streamer.producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 *
 * @author mayconbordin
 */
public class SocketConsumer {
    public static void main(String args[]) throws Exception {
        String host = args[0];
        int port    = Integer.parseInt(args[1]);
        
        Socket client = new Socket(host, port);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
        
        String line;
        long count = 0;
        while ((line = reader.readLine()) != null) {
            //System.out.println(line);
            count++;
        }
        
        System.out.println(count + " lines read");
    }
}
