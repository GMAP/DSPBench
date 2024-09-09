package flink.parsers;

import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 *
 */
public class TransactionParser extends RichFlatMapFunction<String, Tuple2<String, String>> {

    Configuration config;

    Metrics metrics = new Metrics();

    public TransactionParser(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple2<String, String>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] temp = input.split(",", 2);
        if(temp[0] == null || temp[1] == null ){
            out.collect(new Tuple2<>(null, null));
        }else{
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }

            out.collect(new Tuple2<>(temp[0], temp[1]));
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
