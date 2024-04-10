package flink.application.YSB;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;

public class Aggregator implements AggregateFunction<Joined_Event,Aggregate_Event,Aggregate_Event>{
    
    Metrics metric = new Metrics();

    Aggregator(Configuration config){
        metric.initialize(config);
    }

    @Override
    public Aggregate_Event createAccumulator() {
        return new Aggregate_Event();
    }

    @Override
    public Aggregate_Event add(Joined_Event value, Aggregate_Event accumulator) {
        metric.incReceived(this.getClass().getSimpleName());
        return new Aggregate_Event(value.cmp_id, accumulator.count++, value.timestamp);
    }

    @Override
    public Aggregate_Event getResult(Aggregate_Event accumulator) {
        metric.incEmitted(this.getClass().getSimpleName());
        return accumulator;
    }

    @Override
    public Aggregate_Event merge(Aggregate_Event a, Aggregate_Event b) {
        return new Aggregate_Event(a.cmp_id, a.count+b.count, a.timestamp);
    }
}