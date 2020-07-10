package com.streamer.core.impl;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import com.streamer.core.Component;
import com.streamer.core.Stream;
import com.streamer.core.Values;
import java.util.List;

public class StormBoltStream extends Stream {
    private Stream parentStream;
    private OutputCollector collector;

    public StormBoltStream(String id, Stream parentStream) {
        super(id, parentStream.getSchema());

        this.parentStream = parentStream;
    }

    public void setCollector(OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void put(Component component, com.streamer.core.Tuple parent, Values values) {
        com.streamer.core.Tuple tuple = createTuple(component, parent, values);

        collector.emit(id, (Tuple) parent.getTempValue(), tupleToList(tuple));
    }

    public void put(Component component, com.streamer.core.Tuple tuple) {
        List<Object> values = tupleToList(tuple);
        collector.emit(id, values);
    }
}