package org.dspbench.metrics;

import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;

import java.time.Instant;
import java.util.List;

public abstract class MetricsOutputCollector extends OutputCollector {
    public long UnixTime;

    public MetricsOutputCollector(IOutputCollector delegate) {
        super(delegate);
    }

    @Override
    public List<Integer> emit(Tuple anchor, List<Object> tuple) {
        if (this.UnixTime == 0) {
            this.UnixTime = Instant.now().getEpochSecond();
        }
        return super.emit( anchor, tuple);
    }

    @Override
    public List<Integer> emit(List<Object> tuple) {
        if (this.UnixTime == 0) {
            this.UnixTime = Instant.now().getEpochSecond();
        }
        return super.emit(tuple);
    }
}
