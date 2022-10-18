package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.TrafficMonitoringConstants;
import spark.streaming.constants.WordCountConstants;
import spark.streaming.function.SSBeijingTaxiTraceParser;
import spark.streaming.function.SSFilterNull;
import spark.streaming.function.SSMapMatcher;
import spark.streaming.util.Configuration;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import spark.streaming.util.Tuple;

public class TrafficMonitoring extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(TrafficMonitoring.class);
    private String checkpointPath;
    private int batchSize;
    private int parserThreads;
    private int mapMatcherThreads;
    private int speedCalculatorThreads;
    public TrafficMonitoring(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        checkpointPath         = config.get(getConfigKey(TrafficMonitoringConstants.Config.CHECKPOINT_PATH), ".");
        batchSize              = config.getInt(getConfigKey(TrafficMonitoringConstants.Config.BATCH_SIZE), 1000);
        parserThreads          = config.getInt(TrafficMonitoringConstants.Config.PARSER_THREADS, 1);
        mapMatcherThreads      = config.getInt(TrafficMonitoringConstants.Config.MAP_MATCHER_THREADS, 1);
        speedCalculatorThreads = config.getInt(TrafficMonitoringConstants.Config.SPEED_CALCULATOR_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {
        StructType schema = new StructType(new StructField[]{
                new StructField("carId", DataTypes.StringType, false, Metadata.empty()),
                new StructField("date", DataTypes.StringType, false, Metadata.empty()),
                new StructField("occ", DataTypes.BooleanType, false, Metadata.empty()),
                new StructField("lat", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("lon", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("speed", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("bearing", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("roadId", DataTypes.IntegerType, true, Metadata.empty())
        });

        Dataset<Row> rawRecords = createSource();

        Dataset<Row> records = rawRecords
                .as(Encoders.STRING())
                .map(new SSBeijingTaxiTraceParser(config), RowEncoder.apply(schema));

        Dataset<Row> roads = records.filter(new SSFilterNull<>())
                .repartition(mapMatcherThreads).map(new SSMapMatcher(config), RowEncoder.apply(schema));

        return createSink(roads);
    }

    @Override
    public String getConfigPrefix() {
        return TrafficMonitoringConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
