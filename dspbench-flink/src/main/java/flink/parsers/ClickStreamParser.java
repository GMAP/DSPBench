package flink.parsers;

import com.google.gson.Gson;

import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickStreamParser extends RichFlatMapFunction<String, Tuple3<String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    Configuration config;

    Metrics metrics = new Metrics();

    public ClickStreamParser(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple3<String, String, String>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        Gson gson = new Gson();
        try {
            ClickStream clickstream = gson.fromJson(input, ClickStream.class);

            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }

            out.collect(new Tuple3<>(clickstream.ip, clickstream.url, clickstream.clientKey));
        } catch (Exception ex) {
            System.out.println("Error parsing JSON encoded clickStream: " + input + " - " + ex);
        }
        out.collect(new Tuple3<>(null, null, null));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}

class ClickStream {
    public String ip;
    public String url;
    public String clientKey;
}
