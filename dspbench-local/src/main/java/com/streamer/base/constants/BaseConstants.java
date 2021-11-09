package com.streamer.base.constants;

import com.streamer.core.Constants;

/**
 *
 * @author mayconbordin
 */
public interface BaseConstants {
    String BASE_PREFIX = "app";
    
    interface BaseConfig {
        String SOURCE_CLASS                 = "%s.source.class";
        String SOURCE_PATH                  = "%s.source.path";
        String SOURCE_PRODUCER_PREFIX       = "%s.source.producer.prefix";
        String SOURCE_PARSER                = "%s.source.parser";
        String SOURCE_THREADS               = "%s.source.threads";
        String SOURCE_RATE                  = "%s.source.rate";
        String SOURCE_TOPIC                 = "%s.source.topic";
        String SOURCE_GENERATOR             = "%s.source.generator";
        String SOURCE_SOCKET_PORT           = "%s.source.socket.port";
        String SOURCE_SOCKET_HOST           = "%s.source.socket.host";
        String SOURCE_SOCKET_QUEUE_SIZE     = "%s.source.socket.queue_size";
        
        String SINK_CLASS                   = "%s.sink.class";
        String SINK_THREADS                 = "%s.sink.threads";
        String SINK_FORMATTER               = "%s.sink.formatter";
        String SINK_PATH                    = "%s.sink.path";
        String SINK_ENCODING                = "%s.sink.encoding";
        String SINK_SAMPLE_RATE             = "%s.sink.sample.rate";
        String SINK_SOCKET_PORT             = "%s.sink.socket.port";
        String SINK_SOCKET_CHARSET          = "%s.sink.socket.charset";
        
        String REDIS_HOST                   = "%s.redis.server.host";
        String REDIS_PORT                   = "%s.redis.server.port";
        String REDIS_PATTERN                = "%s.redis.server.pattern";
        String REDIS_QUEUE_SIZE             = "%s.redis.server.queue_size";
        String REDIS_SINK_QUEUE             = "%s.redis.sink.queue";
        
        String TWITTER_CONSUMER_KEY         = "%s.twitter.consumer_key";
        String TWITTER_CONSUMER_SECRET      = "%s.twitter.consumer_secret";
        String TWITTER_ACCESS_TOKEN         = "%s.twitter.access_token";
        String TWITTER_ACCESS_TOKEN_SECRET  = "%s.twitter.access_token_secret";
        
        String KAFKA_HOST                   = "%s.kafka.zookeeper.host";
        String KAFKA_SOURCE_TOPIC           = "%s.kafka.source.topic";
        String KAFKA_ZOOKEEPER_PATH         = "%s.kafka.zookeeper.path";
        String KAFKA_CONSUMER_ID            = "%s.kafka.consumer.id";
        
        String CASSANDRA_HOST               = "%s.cassandra.host";
        String CASSANDRA_KEYSPACE           = "%s.cassandra.keyspace";
        String CASSANDRA_SINK_CF            = "%s.cassandra.sink.column_family";
        String CASSANDRA_SINK_ROW_KEY_FIELD = "%s.cassandra.sink.field.row_key";
        String CASSANDRA_SINK_INC_FIELD     = "%s.cassandra.sink.field.increment";
        String CASSANDRA_SINK_ACK_STRATEGY  = "%s.cassandra.sink.ack_strategy";
        
        String DEBUG_ON                     = "debug.on";
        
        String GEOIP_INSTANCE               = "geoip.instance";
        String GEOIP2_DB                    = "geoip2.db";
    }
    
    interface BaseComponent {
        String SOURCE = "source";
        String SINK   = "sink";
    }
    
    interface BaseStream {
        String DEFAULT = Constants.DEFAULT_STREAM;
    }
}
