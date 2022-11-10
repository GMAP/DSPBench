package spark.streaming.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.util.Configuration;

import java.time.Instant;

/**
 * @author luandopke
 */
public class SSSmartGridParser extends BaseFunction implements MapFunction<String, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSSmartGridParser.class);
    private static final int ID_FIELD           = 0;
    private static final int TIMESTAMP_FIELD    = 1;
    private static final int VALUE_FIELD        = 2;
    private static final int PROPERTY_FIELD     = 3;
    private static final int PLUG_ID_FIELD      = 4;
    private static final int HOUSEHOLD_ID_FIELD = 5;
    private static final int HOUSE_ID_FIELD     = 6;


    public SSSmartGridParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String input) throws Exception {
        String[] fields = input.split(",");

        if (fields.length != 7)
            return null;

        try{
            String id = fields[ID_FIELD];
            long timestamp = Long.parseLong(fields[TIMESTAMP_FIELD]);
            double value = Double.parseDouble(fields[VALUE_FIELD]);
            int property = Integer.parseInt(fields[PROPERTY_FIELD]);
            String plugId = fields[PLUG_ID_FIELD];
            String householdId = fields[HOUSEHOLD_ID_FIELD];
            String houseId = fields[HOUSE_ID_FIELD];

            return RowFactory.create(id, timestamp, value, property, plugId, householdId, houseId);

        } catch (NumberFormatException ex) {
            LOG.warn("Error parsing numeric value", ex);
        }

        return null;
    }
}