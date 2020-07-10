package com.streamer.base.source;

import com.streamer.core.Values;
import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.source.parser.Parser;
import com.streamer.utils.ClassLoaderUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class KafkaSource extends BaseSource {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    
    private ConsumerConnector consumer;
    private ConsumerIterator iterator;
    private Parser parser;
    
    protected void initialize() {
        String parserClass = config.getString(getConfigKey(BaseConfig.SOURCE_PARSER));
        String host        = config.getString(getConfigKey(BaseConfig.KAFKA_HOST));
        String topic       = config.getString(getConfigKey(BaseConfig.KAFKA_SOURCE_TOPIC));

        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        if (StringUtils.isBlank(topic)) {
            LOG.error("A topic name has to be provided");
            throw new RuntimeException("A topic name has to be provided");
        }
        
        consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(host, getName()));
        
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        
        iterator = streams.get(0).iterator();
        LOG.info("KafkaSource has started listening to topic {}", topic);
    }

    @Override
    public final boolean hasNext() {
        return true;
    }

    @Override
    public final void nextTuple() {
        if (iterator.hasNext()) {
            MessageAndMetadata<byte[], byte[]> message = iterator.next();
            
            String content = new String(message.message());
            
            if (StringUtils.isNotBlank(content)) {
                List<Values> tuples = parser.parse(content);
                
                if (tuples != null) {
                    for (Values tuple : tuples)
                        emit(tuple.getStreamId(), tuple);
                }
            }
        }
    }
    
    private static ConsumerConfig createConsumerConfig(String zkConnect, String groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnect);
        props.put("group.id", groupId);
        
        // using same values as storm
        props.put("zookeeper.session.timeout.ms", "20000");
        props.put("zookeeper.connection.timeout.ms", "15000");
        
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        
        props.put("rebalance.backoff.ms", "10000");
        props.put("rebalance.max.retries", "6");
 
        return new ConsumerConfig(props);
    }
}
