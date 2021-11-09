package com.streamer.base.source;

import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import static com.streamer.base.constants.BaseConstants.BaseConfig.*;
import com.streamer.utils.ClassLoaderUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class SocketSource extends BaseSource implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SocketSource.class);
    private static final int DEFAULT_PORT = 6860;
    
    protected Parser parser;
    private String host;
    private int port;
    
    private Socket client;
    private BufferedReader reader;
    
    private int queueSize;
    private LinkedBlockingQueue<Values> queue;
    private ExecutorService executor;
    
    @Override
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(SOURCE_PARSER));
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        host = config.getString(getConfigKey(SOURCE_SOCKET_HOST), null);
        port = config.getInt(getConfigKey(SOURCE_SOCKET_PORT), DEFAULT_PORT);
        
        try {
            client = new Socket(host, port);
            reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
        } catch (IOException ex) {
            LOG.error("SocketSpout error: " + ex.getMessage(), ex);
            throw new RuntimeException("SocketSpout error: " + ex.getMessage(), ex);
        }
        /*
        queueSize = config.getInt(getConfigKey(SOURCE_SOCKET_QUEUE_SIZE), Integer.MAX_VALUE);
        queue = new LinkedBlockingQueue<Values>(queueSize);
        
        executor = Executors.newSingleThreadExecutor();
        executor.submit(this);*/
    }

    @Override
    public void nextTuple() {
        /*try {
            Values values = queue.take();
            emit(values.getStreamId(), values);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Unable to put tuples into queue", ex);
        }*/
        
        try {
            String packet = reader.readLine();
            
                 
                //String packet = reader.readLine();
            if (packet != null) {
                List<Values> tuples = parser.parse(packet);

                if (tuples != null) {
                    for (Values values : tuples) {
                        emit(values.getStreamId(), values);
                    }
                }
            }
        } catch (IOException ex) {
            LOG.error("SocketSpout error: " + ex.getMessage(), ex);
            throw new RuntimeException("SocketSource error: " + ex.getMessage(), ex);
        }
        
        /*try {
            String packet;
            
            // ADD BUFFER WITH LIMITED SIZE
            
             while ((packet = reader.readLine()) != null) {
            
                 
                //String packet = reader.readLine();
                if (packet != null) {
                    List<Values> tuples = parser.parse(packet);

                    if (tuples != null) {
                        for (Values values : tuples) {
                            emit(values.getStreamId(), values);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            LOG.error("SocketSpout error: " + ex.getMessage(), ex);
            throw new RuntimeException("SocketSource error: " + ex.getMessage(), ex);
        }*/
        /*while (true) {
            try {
                String packet = reader.readLine();
                if (packet != null) {
                    List<Values> tuples = parser.parse(packet);
        
                    if (tuples != null) {
                        for (Values values : tuples) {
                            emit(values.getStreamId(), values);
                        }
                    }
                }
            } catch (IOException ex) {
                LOG.error("SocketSpout error: " + ex.getMessage(), ex);
                throw new RuntimeException("SocketSpout error: " + ex.getMessage(), ex);
            }
        }*/
    }

    @Override
    public void onDestroy() {
        super.onDestroy();
        
        try {
            reader.close();
            client.close();
        } catch (IOException ex) {
            LOG.error("Error while closing socket server", ex);
        }
        
        executor.shutdown();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    public void run() {
        try {
            String packet;
             while ((packet = reader.readLine()) != null) {
                if (packet != null) {
                    List<Values> tuples = parser.parse(packet);

                    if (tuples != null) {
                        for (Values values : tuples) {
                            queue.put(values);
                        }
                    }
                }
            }
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read data from socket", ex);
        } catch (InterruptedException ex) {
            throw new RuntimeException("Unable to put tuples into queue", ex);
        }
    }
}
