package com.streamer.topology.impl;

import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import com.codahale.metrics.MetricRegistry;
import com.streamer.base.constants.BaseConstants;
import com.streamer.base.constants.BaseConstants.BaseConfig;
import com.streamer.base.source.KafkaSource;
import com.streamer.core.Operator;
import com.streamer.core.Schema;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.core.impl.IStormSpout;
import com.streamer.core.impl.StormKafkaSpout;
import com.streamer.core.impl.StormSpout;
import com.streamer.core.impl.StormStream;
import com.streamer.topology.ComponentFactory;
import com.streamer.topology.IOperatorAdapter;
import com.streamer.topology.ISourceAdapter;
import com.streamer.topology.Topology;
import com.streamer.utils.Configuration;
import java.util.UUID;
import storm.kafka.BrokerHosts;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

/**
 *
 * @author mayconbordin
 */
public class StormComponentFactory implements ComponentFactory {
    private final TopologyBuilder builder;
    private MetricRegistry metrics;
    private Configuration configuration;

    public StormComponentFactory(TopologyBuilder builder) {
        this.builder = builder;
    }

    public Stream createStream(String name, Schema schema) {
        return new StormStream(name, schema);
    }

    public IOperatorAdapter createOperatorAdapter(String name, Operator operator) {
        StormOperatorAdapter adapter = new StormOperatorAdapter();
        adapter.setComponent(operator);
        
        BoltDeclarer declarer = builder.setBolt(name, adapter.getBolt(), operator.getParallelism());
        adapter.setDeclarer(declarer);
        
        return adapter;
    }

    public ISourceAdapter createSourceAdapter(String name, Source source) {
        IStormSpout spout = null;
        
        if (source instanceof KafkaSource) {
            KafkaSource kafkaSource = (KafkaSource) source;
            String host  = configuration.getString(kafkaSource.getConfigKey(BaseConfig.KAFKA_HOST));
            String topic = configuration.getString(kafkaSource.getConfigKey(BaseConfig.KAFKA_SOURCE_TOPIC));
            
            BrokerHosts zkHosts = new ZkHosts(host);
            SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/" + topic, UUID.randomUUID().toString());
            spoutConfig.scheme = new SchemeAsMultiScheme(new StormKafkaSpout.ParserSchema());
            spoutConfig.bufferSizeBytes = 10 * 1024 * 1024;
            spoutConfig.fetchSizeBytes  = 10 * 1024 * 1024;
            
            spout = new StormKafkaSpout(spoutConfig);
        } else {
            spout = new StormSpout();
        }
        
        StormSourceAdapter adapter = new StormSourceAdapter();
        adapter.setSpout(spout);
        adapter.setComponent(source);
        
        SpoutDeclarer declarer = builder.setSpout(name, adapter.getSpout(), source.getParallelism());
        adapter.setDeclarer(declarer);
        
        return adapter;
    }

    public Topology createTopology(String name) {
        return new StormTopology(name, configuration);
    }

    public void setMetrics(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
    
}
