package spark.streaming.function;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.util.Configuration;

import java.time.Instant;
import java.util.UUID;

/**
 * @author luandopke
 */
public class ClickStreamParser extends BaseFunction implements MapFunction<String, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    public ClickStreamParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String value) throws Exception {

        try {
            ClickStream clickstream = new Gson().fromJson(value, ClickStream.class);
            return RowFactory.create(clickstream.ip,
                    clickstream.url,
                    clickstream.clientKey);
        } catch (JsonSyntaxException ex) {
            LOG.error("Error parsing JSON encoded clickstream: " + value, ex);
        }

        return RowFactory.create();
    }

    private static class ClickStream {
        public String ip;
        public String url;
        public String clientKey;
    }
}