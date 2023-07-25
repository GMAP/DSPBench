package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;

/**
 */
public class JsonTweetParser extends JsonParser implements MapFunction<String, Tuple4<String, String, Date, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTweetParser.class);
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    Configuration config;

    public JsonTweetParser(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    public Tuple4<String, String, Date, String> map(String input) {
        super.initialize(config);
        super.incBoth();

        Tuple1<JSONObject> parsed = super.parse(input);

        JSONObject tweet = parsed.getField(0);

        if (tweet.containsKey("data")) {
            tweet = (JSONObject) tweet.get("data");
        }

        if (!tweet.containsKey("id") || !tweet.containsKey("text") || !tweet.containsKey("created_at"))
            return null;

        String id = (String) tweet.get("id");
        String text = (String) tweet.get("text");
        DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

        return new Tuple4<String, String, Date, String>(id, text, timestamp.toDate(),
                Instant.now().toEpochMilli() + "");
    }
}
