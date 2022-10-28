package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.Date;

/**
 * @author mayconbordin
 */
public class JsonTweetParser extends JsonParser  implements MapFunction<String, Tuple4<String, String, Date, String>> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTweetParser.class);
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public Tuple4<String, String, Date, String> map(String input) {
        Tuple1<JSONObject> parsed = super.parse(input);

        JSONObject tweet = (JSONObject) parsed.getField(0);

        if (tweet.containsKey("data")) {
            tweet = (JSONObject) tweet.get("data");
        }

        if (!tweet.containsKey("id") || !tweet.containsKey("text") || !tweet.containsKey("created_at"))
            return null;

        String id = (String) tweet.get("id");
        String text = (String) tweet.get("text");
        DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get("created_at"));

        return new Tuple4<String, String, Date, String>(id, text, timestamp.toDate(), Instant.now().toEpochMilli() + "");
    }
}
