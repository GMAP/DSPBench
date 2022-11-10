package spark.streaming.function;

import com.google.common.collect.ImmutableList;
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
 * @author luandopke
 */
public class SSTransationParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSTransationParser.class);

    public SSTransationParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String value) throws Exception {
        super.calculateThroughput();
        try {
            String[] items = value.split(",", 2);

            return RowFactory.create(items[0], items[1], Instant.now().toEpochMilli());
        } catch (NumberFormatException ex) {
            LOG.error("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.error("Error parsing date/time value", ex);
        }

        return RowFactory.create();
    }
}