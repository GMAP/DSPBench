package com.streamer.core.impl;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import com.codahale.metrics.MetricRegistry;
import com.streamer.core.Source;
import com.streamer.core.Stream;
import static com.streamer.topology.impl.StormConstants.TUPLE_FIELD;
import com.streamer.utils.Configuration;
import com.streamer.metrics.MetricsFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormSpout extends BaseRichSpout implements IStormSpout {
    private static final Logger LOG = LoggerFactory.getLogger(StormSpout.class);
    
    private Source source;
    private Map<String, Stream> streams;

    public StormSpout() {
        streams = new HashMap<String, Stream>();
    }

    public void setSource(Source source) {
        this.source = source;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Stream> e : source.getOutputStreams().entrySet()) {
            List<String> fields = new ArrayList<String>(e.getValue().getSchema().getKeys());
            fields.add(TUPLE_FIELD);
            declarer.declareStream(e.getKey(), new Fields(fields));
        }
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        Configuration config = Configuration.fromMap(conf);
        source.onCreate(Math.abs(UUID.randomUUID().hashCode()), config);

        for (Map.Entry<String, Stream> e : source.getOutputStreams().entrySet()) {
            Stream parent = e.getValue();
            StormSpoutStream spoutStream = new StormSpoutStream(parent.getStreamId(), parent);
            spoutStream.setCollector(collector);
            streams.put(e.getKey(), spoutStream);
        }

        source.setOutputStreams(streams);

        MetricRegistry metrics = MetricsFactory.createRegistry(config);
        if (metrics != null) {
            String name = source.getFullName();
            source.addHooks(MetricsFactory.createMetricHooks(source, metrics, name));
            
            LOG.info("metrics registered for component {}", name);
        }
    }

    public void nextTuple() {
        if (source.hasNext()) {
            source.hooksBefore(null);
            source.nextTuple();
            source.hooksAfter(null);
            source.hooksOnReceive(null);
        }
    }
}