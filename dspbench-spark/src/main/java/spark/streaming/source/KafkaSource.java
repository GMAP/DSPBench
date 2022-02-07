package spark.streaming.source;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
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
        List<String> topics = new ArrayList<>(Arrays.asList(kafkaTopics.split(",")));
        HashMap<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", kafkaHost);

        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(context,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<Tuple2<String, Tuple>> lines = messages.map(new KafkaParser(config));
        
        return lines;
    }
    
}
