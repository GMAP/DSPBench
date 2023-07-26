package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;

/**
 */
public class JsonTweetParser extends JsonParser implements MapFunction<String, Tuple3<String, String, Date>> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTweetParser.class);
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat
            .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    Configuration config;

    public JsonTweetParser(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    public Tuple3<String, String, Date> map(String input) {
        super.initialize(config);
        super.incBoth();

        // Tuple1<JSONObject> parsed = super.parse(input);

        JSONObject tweet = new JSONObject(input);// parsed.getField(0);

        if (tweet.has("data")) {
            tweet = (JSONObject) tweet.get("data");
        }

        if (!tweet.has("id") || !tweet.has("text") || !tweet.has("created_at"))
            return null;

        String id = (String) tweet.get("id");
        String text = (String) tweet.get("text");
        DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

        return new Tuple3<String, String, Date>(id, text, timestamp.toDate());
    }
}
