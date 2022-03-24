package com.streamer.producer;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public abstract class AbstractProducer {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractProducer.class);
    
    protected int id;
    
    protected String path;
    protected File[] files;
    protected BufferedReader reader;
    
    protected long totalBytes = 0;
    protected long readBytes = 0;
    protected long msgCount = 0;
    
    protected final Properties properties;
    protected final StatsDClient statsd;
 
    public AbstractProducer(Properties properties, String path) {
        this.properties = properties;
        this.path = path;

        buildIndex();
        
        id = Math.abs(UUID.randomUUID().hashCode());
        
        String prefix = String.format("%s-%d", properties.getProperty("statsd.prefix", "streamer.producer"), id);
        String host   = properties.getProperty("statsd.host", "localhost");
        int port      = Integer.parseInt(properties.getProperty("statsd.port", "8125"));
        
        statsd = new NonBlockingStatsDClient(prefix, host, port);
    }
    
    protected final void buildIndex() {
        File dir = new File(path);
        if (!dir.exists()) {
            throw new RuntimeException("The source path '" + path + "' does not exists");
        }
        
        if (dir.isDirectory()) {
            files = dir.listFiles();
        } else {
            files = new File[1];
            files[0] = dir;
        }
        
        for (File f : files) {
            totalBytes += f.length();
        }
        
        getLogger().info("Producer has to read {} files, totalizing {}", files.length, humanReadableByteCount(totalBytes));
    }
    
    protected abstract Logger getLogger();
    
    protected static Properties loadProperties(String filename) {
        Properties p = new Properties();
        InputStream is = null;
        
        try {
            is = new FileInputStream(filename);
            p.load(is);
            return p;
        } catch (FileNotFoundException ex) {
            LOG.error("File " + filename + " was not found", ex);
        } catch (IOException ex) {
            LOG.error("Error reading " + filename + " file", ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    LOG.error("Error closing " + filename + " file", ex);
                }
            }
        }
        
        return null;
    }
    
    protected String humanReadableByteCount(long bytes) {
        return humanReadableByteCount(bytes, false);
    }
    
    protected String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }
}
