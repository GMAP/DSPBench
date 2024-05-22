package flink.source;

import flink.constants.BaseConstants;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class KafkasSource extends BaseSource{

    private static final Logger LOG = LoggerFactory.getLogger(KafkasSource.class);

    private String brokers;
    private String topic;
    private final String groupId = UUID.randomUUID().toString();
    private int sourceThreads;

    @Override
    public void initialize(Configuration config, StreamExecutionEnvironment env, String prefix) {
        super.initialize(config, env, prefix);
        brokers = config.getString(String.format(BaseConstants.BaseConf.KAFKA_HOST, prefix),"localhost:9092");
        topic = config.getString(String.format(BaseConstants.BaseConf.KAFKA_SOURCE_TOPIC, prefix),"books");
        sourceThreads = config.getInteger(String.format(BaseConstants.BaseConf.SOURCE_THREADS, prefix), 1);

    }

    @Override
    public DataStream<String> createStream() {

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(sourceThreads);
    }
}
