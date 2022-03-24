package com.streamer.producer;

import static com.streamer.producer.AbstractProducer.loadProperties;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class SocketProducer extends AbstractProducer implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(SocketProducer.class);

    private final PrintWriter writer;
    
    public SocketProducer(Socket clientSocket, Properties properties, String path) throws IOException {
        super(properties, path);
        this.path = path;
        
        writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream())), true);
    }
    
    public void run() {
        try {
            for (File file : files) {
                reader = new BufferedReader(new FileReader(file));
                LOG.info("Opened file {}, size {}", file.getName(), humanReadableByteCount(file.length()));

                String line;
                int i = 0;
                while ((line = reader.readLine()) != null) {
                    //os.write(line.getBytes());
                    writer.println(line);
                    i++;
                }
                
                writer.flush();
                reader.close();
                
                readBytes += file.length();
                msgCount  += i; 
                
                statsd.gauge("read", readBytes);
                statsd.gauge("progress", ((double)readBytes*100.0)/(double)totalBytes);
                statsd.gauge("messages", msgCount);
            }
            
            writer.close();
            LOG.info("Producer finished, sent {} messages", msgCount);
        } catch (FileNotFoundException ex) {
            LOG.error("Unable to find file", ex);
        } catch (IOException ex) {
            LOG.error("Unable to read file", ex);
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }
    
    public static void main(String args[]) throws Exception {
        if (args.length < 3) {
            usage();
        }
        
        String command = args[0];
        Properties props = loadProperties(args[1]);
        
        if (command.equals("start")) {
            if (args.length != 3) {
                usage();
            }

            String dir = args[2];
            int port = Integer.parseInt(props.getProperty("producer.port", "8080"));
            
            ExecutorService executor = Executors.newFixedThreadPool(5);
            ServerSocket serverSocket = new ServerSocket(port);
            
            while (true) {
                Socket clientSocket = serverSocket.accept();
                executor.submit(new SocketProducer(clientSocket, props, dir));
            }
        }
        
        else if (command.equals("stop")) {
            
        }
    }
    
    private static void usage() {
        System.err.println("Usage: producer COMMAND");
        System.err.println("Commands:");
        System.err.println("  start <properties-file> <data-dir>");
        System.err.println("  stop  <properties-file>");
        System.exit(1);
    }
}
