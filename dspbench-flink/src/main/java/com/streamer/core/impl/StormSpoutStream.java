package com.streamer.core.impl;

import backtype.storm.spout.SpoutOutputCollector;
import com.streamer.core.Component;
import com.streamer.core.Stream;
import com.streamer.core.Tuple;
import java.util.List;

public class StormSpoutStream extends Stream {
    private Stream parentStream;
    private SpoutOutputCollector collector;

    public StormSpoutStream(String id, Stream parentStream) {
        super(id, parentStream.getSchema());
        this.parentStream = parentStream;
    }

    public void setCollector(SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void put(Component component, Tuple tuple) {
        List<Object> values = tupleToList(tuple);
        if (tuple.getTempValue() == null)
            collector.emit(id, values, tuple.getId());
        else
            collector.emit(id, values, tuple.getTempValue());
    }
}