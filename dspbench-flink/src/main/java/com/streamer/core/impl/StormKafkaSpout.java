package com.streamer.core.impl;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.streamer.base.constants.BaseConstants;
import com.streamer.base.source.KafkaSource;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import com.streamer.core.Values;
import static com.streamer.topology.impl.StormConstants.TUPLE_FIELD;
import com.streamer.utils.ClassLoaderUtils;
import com.streamer.utils.Configuration;
import com.streamer.metrics.MetricsFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;

/**
 *
 * @author mayconbordin
 */
public class StormKafkaSpout extends KafkaSpout implements IStormSpout {
    private static final Logger LOG = LoggerFactory.getLogger(StormKafkaSpout.class);
    
    private KafkaSource source;
    
    public StormKafkaSpout(SpoutConfig spoutConf) {
        super(spoutConf);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        Configuration config = Configuration.fromMap(conf);
        source.setId(Math.abs(UUID.randomUUID().hashCode()));

        Map<String, Stream> streams = new HashMap<String, Stream>();
        for (Map.Entry<String, Stream> e : source.getOutputStreams().entrySet()) {
            Stream parent = e.getValue();
            StormSpoutStream spoutStream = new StormSpoutStream(parent.getStreamId(), parent);
            spoutStream.setCollector(collector);
            streams.put(e.getKey(), spoutStream);
        }

        MetricRegistry metrics = MetricsFactory.createRegistry(config);
        if (metrics != null) {
            String name = source.getFullName();
            source.addHooks(MetricsFactory.createMetricHooks(source, metrics, name));
            LOG.info("metrics registered for component {}", name);
        }
        
        String parserClass = config.getString(source.getConfigKey(BaseConstants.BaseConfig.SOURCE_PARSER));
        Parser parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        
        super.open(conf, context, new OutputCollector(source, parser, streams));
    }
    
    @Override
    public void nextTuple() {
        source.hooksBefore(null);
        super.nextTuple();
        source.hooksAfter(null);
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Stream> e : source.getOutputStreams().entrySet()) {
            List<String> fields = new ArrayList<String>(e.getValue().getSchema().getKeys());
            fields.add(TUPLE_FIELD);
            declarer.declareStream(e.getKey(), new Fields(fields));
        }
    }

    public void setSource(Source source) {
        this.source = (KafkaSource) source;
    }
    
    private static final class OutputCollector extends SpoutOutputCollector {
        private Map<String, Stream> streams;
        private Source source;
        private Parser parser;
        
        public OutputCollector(Source source, Parser parser, Map<String, Stream> streams) {
            super(null);
            
            this.source = source;
            this.parser = parser;
            this.streams = streams;
        }

        @Override
        public void emitDirect(int taskId, List<Object> tuple) {}
        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple) {}
        @Override
        public void emitDirect(int taskId, List<Object> tuple, Object messageId) {}
        @Override
        public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {}
        @Override
        public List<Integer> emit(String streamId, List<Object> tuple) {return null;}
        @Override
        public List<Integer> emit(List<Object> tuple) {return null;}
        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {return null;}
        
        @Override
        public List<Integer> emit(List<Object> tuple, Object messageId) {
            String content = (String) tuple.get(0);
            source.hooksOnReceive(tuple);
            
            List<Values> tuples = parser.parse(content);
                
            if (tuples != null) {
                for (Values t : tuples) {
                    t.setTempValue(messageId);
                    source.hooksOnEmit(t);
                    streams.get(t.getStreamId()).put(source, t);
                }
            }
            
            return null;
        }
    }
    
    public static final class ParserSchema extends StringScheme {
        @Override
        public List<Object> deserialize(byte[] bytes) {
            String value = deserializeString(bytes);
            return ImmutableList.of((Object)value);
        }
    }
}
