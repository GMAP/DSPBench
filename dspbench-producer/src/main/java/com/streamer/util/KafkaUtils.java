package com.streamer.util;

import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    
    private static ZkClient zkClient;

    public static void setZookeeper(String zk) {
        LOG.info("Zookeeper servers: {}", zk);
        zkClient = new ZkClient(zk, 30000, 30000, new ZKStringSerializerWrapper());
    }

    public static void createKafkaTopic(String name, int partitions, int replicas, boolean overwrite) {
        LOG.info("Creating kafka topic {} with {} partitions and {} replicas", name, partitions, replicas);
        
        if (AdminUtils.topicExists(zkClient, name)) {
            if (overwrite) {
                LOG.info("Kafka topic {} already exists, deleting topic", name);
                deleteKafkaTopic(name);
            } else {
                LOG.info("Kafka topic {} already exists, leave as it is", name);
                return;
            }
        }

        AdminUtils.createTopic(zkClient, name, partitions, replicas, new Properties());
    }
    
    public static void deleteKafkaTopic(String name) {
        LOG.info("Deleting kafka topic {}", name);
        
        ZkUtils.deletePathRecursive(zkClient, ZkUtils.getTopicPath(name));
    }

    public static class ZKStringSerializerWrapper implements ZkSerializer {
        @Override
        public Object deserialize(byte[] byteArray) throws ZkMarshallingError {
            return ZKStringSerializer.deserialize(byteArray);
        }

        @Override
        public byte[] serialize(Object obj) throws ZkMarshallingError {
                return ZKStringSerializer.serialize(obj);
        }
    }
}