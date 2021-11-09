package spark.streaming.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import spark.streaming.constants.BaseConstants.BaseConfig;
import spark.streaming.function.KafkaParser;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class KafkaSource extends BaseSource {
    private String kafkaTopics;
    private String kafkaHost;
    
    @Override
    public void initialize(Configuration config, JavaStreamingContext context, String prefix) {
        super.initialize(config, context, prefix);
        
        kafkaTopics = config.get(String.format(BaseConfig.KAFKA_SOURCE_TOPIC, prefix));
        kafkaHost   = config.get(String.format(BaseConfig.KAFKA_HOST, prefix));
    }
    
    @Override
    public JavaDStream<Tuple2<String, Tuple>> createStream() {
        HashSet<String> topicsSet = new HashSet<>(Arrays.asList(kafkaTopics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaHost);
        
        JavaPairInputDStream<String, String> messages =  KafkaUtils.createDirectStream(
                context, String.class, String.class, StringDecoder.class, StringDecoder.class,
                kafkaParams, topicsSet);
        
        JavaDStream<Tuple2<String, Tuple>> lines = messages.map(new KafkaParser(config));
        
        return lines;
    }
    
}
