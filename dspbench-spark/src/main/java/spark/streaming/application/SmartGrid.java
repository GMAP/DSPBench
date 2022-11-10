package spark.streaming.application;

import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.LogProcessingConstants;
import spark.streaming.constants.SmartGridConstants;
import spark.streaming.function.*;
import spark.streaming.model.CountryStats;
import spark.streaming.util.Configuration;

public class SmartGrid extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SmartGrid.class);

    private int parserThreads;

    public SmartGrid(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(LogProcessingConstants.Config.PARSER_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() throws StreamingQueryException {

        var rawRecords = createSource();

        var records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSSmartGridParser(config), Encoders.kryo(Row.class));

        return createSink(records);
    }

    @Override
    public String getConfigPrefix() {
        return SmartGridConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
