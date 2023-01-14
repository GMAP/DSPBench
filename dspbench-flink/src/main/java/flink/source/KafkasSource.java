package flink.source;

import flink.constants.BaseConstants;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.UUID;

public class KafkasSource extends BaseSource{
    private String brokers;
    private String topic;
    private final String groupId = UUID.randomUUID().toString();

    @Override
    public void initialize(Configuration config, StreamExecutionEnvironment env, String prefix) {
        super.initialize(config, env, prefix);
        brokers = config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092");
        topic = config.getString(String.format(BaseConstants.BaseConf.KAFKA_SOURCE_TOPIC, prefix),"books");
    }

    @Override
    public DataStream<String> createStream() {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }
}
