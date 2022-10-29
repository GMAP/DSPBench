package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author luandopke
 */
public class SSBeijingTaxiTraceParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSBeijingTaxiTraceParser.class);
    private static final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");

    private static final int ID_FIELD = 0;
    private static final int NID_FIELD = 1;
    private static final int DATE_FIELD = 2;
    private static final int LAT_FIELD = 3;
    private static final int LON_FIELD = 4;
    private static final int SPEED_FIELD = 5;
    private static final int DIR_FIELD = 6;

    public SSBeijingTaxiTraceParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String value) throws Exception {
        String[] fields = value.toString().replace("\"", "").split(",");
        if (fields.length != 7)
            return RowFactory.create();

        try {
            String carId = fields[ID_FIELD];
            DateTime date = formatter.parseDateTime(fields[DATE_FIELD]);

            //int msgId = String.format("%s:%s", carId, date.toString()).hashCode();

            //incEmitted();
            return RowFactory.create(carId,
                    date.toString(),
                    true,
                    Double.parseDouble(fields[LAT_FIELD]),
                    Double.parseDouble(fields[LON_FIELD]),
                    ((Double) Double.parseDouble(fields[SPEED_FIELD])).intValue(),
                    Integer.parseInt(fields[DIR_FIELD]),
                    null);
        } catch (NumberFormatException ex) {
            LOG.error("Error parsing numeric value", ex);
        } catch (IllegalArgumentException ex) {
            LOG.error("Error parsing date/time value", ex);
        }

        return RowFactory.create();
    }
}