package com.streamer.core.impl;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.codahale.metrics.MetricRegistry;
import com.streamer.core.Operator;
import com.streamer.core.Sink;
import com.streamer.core.Stream;
import static com.streamer.topology.impl.StormConstants.TUPLE_FIELD;
import com.streamer.util.TupleHelpers;
import com.streamer.utils.Configuration;
import com.streamer.metrics.MetricsFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StormBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(StormBolt.class);
    
    private Operator operator;
    private OutputCollector collector;
    private long timeInterval;
    private Map<String, Stream> streams;

    public StormBolt() {
        streams = new HashMap<String, Stream>();
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public void setTimeInterval(long timeInterval) {
        this.timeInterval = timeInterval;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Map.Entry<String, Stream> e : operator.getOutputStreams().entrySet()) {
            List<String> fields = new ArrayList<String>(e.getValue().getSchema().getKeys());
            fields.add(TUPLE_FIELD);
            declarer.declareStream(e.getKey(), new Fields(fields));
        }
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector outputCollector) {
        collector = outputCollector;
        Configuration config = Configuration.fromMap(stormConf);
        operator.onCreate(Math.abs(UUID.randomUUID().hashCode()), config);

        for (Map.Entry<String, Stream> e : operator.getOutputStreams().entrySet()) {
            Stream parent = e.getValue();
            StormBoltStream boltStream = new StormBoltStream(parent.getStreamId(), parent);
            boltStream.setCollector(collector);
            streams.put(e.getKey(), boltStream);
        }

        operator.setOutputStreams(streams);

        MetricRegistry metrics = MetricsFactory.createRegistry(config);
        if (metrics != null) {
            String name = operator.getFullName();
            operator.addHooks(MetricsFactory.createMetricHooks(operator, metrics, name));
            LOG.info("metrics registered for component {}", name);
        }
    }

    public void execute(Tuple input) {
        if (TupleHelpers.isTickTuple(input)) {
            operator.hooksBefore(null);
            operator.onTime();
            operator.hooksAfter(null);
        } else {
            com.streamer.core.Tuple tuple = (com.streamer.core.Tuple) input.getValueByField(TUPLE_FIELD);
            tuple.setTempValue(input);

            operator.hooksBefore(tuple);
            operator.process(tuple);
            operator.hooksAfter(tuple);
        }

        collector.ack(input);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        if (timeInterval > 0) {
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timeInterval);
        }
        return conf;
    }
}