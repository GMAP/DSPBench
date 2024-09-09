package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Configurations;
import flink.util.Metrics;

import java.util.Date;

/**
 */
public class JsonTweetParser extends RichFlatMapFunction<String, Tuple3<String, String, Date>> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTweetParser.class);
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    Configuration config;
    Metrics metrics = new Metrics();

    public JsonTweetParser(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple3<String, String, Date>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        // Tuple1<JSONObject> parsed = super.parse(input);

        JSONObject tweet = new JSONObject(input);// parsed.getField(0);

        if (tweet.has("data")) {
            tweet = (JSONObject) tweet.get("data");
        }

        if (!tweet.has("id") || !tweet.has("text") || !tweet.has("created_at"))
            out.collect(new Tuple3<>(null, null, null));

        String id = (String) tweet.get("id");
        String text = (String) tweet.get("text");
        DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }

        out.collect(new Tuple3<String, String, Date>(id, text, timestamp.toDate()));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
