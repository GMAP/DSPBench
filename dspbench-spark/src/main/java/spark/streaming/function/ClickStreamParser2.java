package spark.streaming.function;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.util.Configuration;

import java.time.Instant;

/**
 * @author luandopke
 */
public class ClickStreamParser2 extends BaseFunction implements MapFunction<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser2.class);

    public ClickStreamParser2(Configuration config) {
        super(config);
    }

    @Override
    public Row call(Row value) throws Exception {

            return RowFactory.create(value.get(0),
                    value.get(1),
                    value.get(2), value.get(3), "2");

    }

    private static class ClickStream {
        public String ip;
        public String url;
        public String clientKey;
    }
}