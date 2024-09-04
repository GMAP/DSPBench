package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Configurations;
import flink.util.Metrics;

public class LearnerParser extends RichFlatMapFunction<String, Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerParser.class);

    Configuration config;
    String sourceName;
    Metrics metrics = new Metrics();

    public LearnerParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    @Override
    public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String[] temp = input.split(",");
        if(temp[0] == null || temp[1] == null){
            out.collect(new Tuple2<>(null, null));
        }
        else{
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            out.collect(new Tuple2<String, Integer>(temp[0], Integer.parseInt(temp[1])));
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
    
}
