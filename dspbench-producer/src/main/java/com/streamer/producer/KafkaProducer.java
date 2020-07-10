package com.streamer.producer;

import com.streamer.util.KafkaUtils;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class KafkaProducer extends AbstractProducer implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
             
    private String topic;
    private Producer<String, String> producer;
 
    public KafkaProducer(Properties properties, String topic, String path) {
        super(properties, path);
        this.topic = topic;

        ProducerConfig config = new ProducerConfig(properties);
        producer = new Producer<String, String>(config);
    }

    public static void main(String args[]) throws InterruptedException {
        if (args.length < 3) {
            usage();
        }
        
        String command = args[0];
        String topic = args[2];
        Properties props = loadProperties(args[1]);
        
        // set broker list
        KafkaUtils.setZookeeper(props.getProperty("zk.connect"));
        
        if (command.equals("start")) {
            if (args.length != 6) {
                usage();
            }
            
            //int partitions = Integer.parseInt(args[3]);
            //int replicas   = Integer.parseInt(args[4]);
            String dir = args[5];
            
            // create topic
            //KafkaUtils.createKafkaTopic(topic, partitions, replicas, false);

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(new KafkaProducer(props, topic, dir));
        }
        
        else if (command.equals("create_topic")) {
            int partitions = Integer.parseInt(args[3]);
            int replicas   = Integer.parseInt(args[4]);
            
            // create topic
            KafkaUtils.createKafkaTopic(topic, partitions, replicas, true);
        }
        
        else if (command.equals("stop")) {
            KafkaUtils.deleteKafkaTopic(topic);
        }
    }
    
    private static void usage() {
        System.err.println("Usage: producer COMMAND");
        System.err.println("Commands:");
        System.err.println("  start <properties-file> <topic> <partitions> <replicas> <data-dir>");
        System.err.println("  stop  <properties-file> <topic>");
        System.exit(1);
    }
 
    public void run() {
        try {
            for (File file : files) {
                reader = new BufferedReader(new FileReader(file));
                LOG.info("Opened file {}, size {}", file.getName(), humanReadableByteCount(file.length()));

                String line;
                int i = 0;
                while ((line = reader.readLine()) != null) {
                    KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, String.valueOf(i), line);
                    producer.send(data);
                    i++;
                }
                
                readBytes += file.length();
                msgCount  += i; 
                
                statsd.gauge("read", readBytes);
                statsd.gauge("progress", ((double)readBytes*100.0)/(double)totalBytes);
                statsd.gauge("messages", msgCount);
            }
            
            // just to have sure
            statsd.gauge("progress", 100);
            
            statsd.stop();
            
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
}
