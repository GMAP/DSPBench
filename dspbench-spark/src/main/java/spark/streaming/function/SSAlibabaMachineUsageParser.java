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
import spark.streaming.model.MachineMetadata;
import spark.streaming.util.Configuration;

import java.time.Instant;

/**
 * @author luandopke
 */
public class SSAlibabaMachineUsageParser extends BaseFunction implements MapFunction<String, Row> {
    private static final int TIMESTAMP = 1;
    private static final int MACHINE_ID = 0;
    private static final int CPU = 2;
    private static final int MEMORY = 3;

    public SSAlibabaMachineUsageParser(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String value) throws Exception {
        String[] items = value.split(",");

        if (items.length != 9)
            return null;

        String id = items[MACHINE_ID];
        long timestamp = Long.parseLong(items[TIMESTAMP]) * 1000;
        double cpu = Double.parseDouble(items[CPU]);
        double memory = Double.parseDouble(items[MEMORY]);

        return RowFactory.create(id,
                timestamp,
                new MachineMetadata(timestamp, id, cpu, memory)
        );
    }
}