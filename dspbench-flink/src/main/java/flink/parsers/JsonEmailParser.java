package flink.parsers;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonSyntaxException;

import flink.util.Configurations;
import flink.util.Metrics;

public class JsonEmailParser extends RichFlatMapFunction<String, Tuple3<String, String, Boolean>>{

    private static final Logger LOG = LoggerFactory.getLogger(JsonEmailParser.class);

    //private final Gson gson = new Gson();
    Configuration config;
    String sourceName;
    Metrics metrics = new Metrics();

    public JsonEmailParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    @Override
    public void flatMap(String input, Collector<Tuple3<String, String, Boolean>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        try {
            //Don't know what makes an email a Spam
            JSONObject email = new JSONObject(input);

            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }
            out.collect(new Tuple3<String, String, Boolean>((String) email.get("id"), (String) email.get("message"), (Boolean) email.get("spam")));
        } catch (JsonSyntaxException ex) {
            LOG.error("Error parsing JSON encoded email", ex);
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
