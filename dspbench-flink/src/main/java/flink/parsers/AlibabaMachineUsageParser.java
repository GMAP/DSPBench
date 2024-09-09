package flink.parsers;

import flink.application.machineoutiler.MachineMetadata;
import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlibabaMachineUsageParser extends RichFlatMapFunction<String, Tuple3<String, Long, MachineMetadata>> {

    private static final Logger LOG = LoggerFactory.getLogger(AlibabaMachineUsageParser.class);

    Configuration config;
    Metrics metrics = new Metrics();

    public AlibabaMachineUsageParser(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple3<String, Long, MachineMetadata>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] temp = input.split(",");
        if(temp[0] == null || temp[1] == null || temp[2] == null || temp[3] == null){
            out.collect(new Tuple3<>(null, null, null));
        }
        else{
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            
            out.collect(new Tuple3<>(
                    temp[0],
                    Long.parseLong(temp[1]) * 1000,
                    new MachineMetadata(
                        Long.parseLong(temp[1]) * 1000, 
                        temp[0], 
                        Double.parseDouble(temp[2]), 
                        Double.parseDouble(temp[3])
                    )
                )
            );
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
