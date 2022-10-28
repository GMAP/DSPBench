package spark.streaming.function;

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

/**
 * @author mayconbordin
 */
public class SSTweetParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSTweetParser.class);
    private static final String ID_FIELD = "id";
    private static final String TEXT_FIELD = "text";
    private static final String DATE_FIELD = "created_at";
    private static final String DATA_FIELD = "data";
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    JSONParser parser;

    public SSTweetParser(Configuration config) {
        super(config);

    }

    @Override
    public Row call(String value) throws Exception {

        try {
            value = value.trim();

            if (value.isEmpty() || (!value.startsWith("{") && !value.startsWith("[")))
                return RowFactory.create();

            parser = new JSONParser();//todo ajustar pra ser global
            JSONObject tweet = (JSONObject) parser.parse(value);

            if (tweet.containsKey(DATA_FIELD)) {
                tweet = (JSONObject) tweet.get(DATA_FIELD);
            }

            if (!tweet.containsKey(ID_FIELD) || !tweet.containsKey(TEXT_FIELD) || !tweet.containsKey(DATE_FIELD))
                return RowFactory.create();

            String id = (String) tweet.get(ID_FIELD);
            String text = (String) tweet.get(TEXT_FIELD);
            DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get(DATE_FIELD));

            return RowFactory.create(id,
                    text,
                    timestamp.toDate());

        } catch (NumberFormatException ex) {
            LOG.error("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.error("Error parsing date/time value", ex);
        }

        return RowFactory.create();
    }
}