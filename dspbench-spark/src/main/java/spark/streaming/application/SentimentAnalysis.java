package spark.streaming.application;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.SentimentAnalysisConstants;
import spark.streaming.constants.TrafficMonitoringConstants;
import spark.streaming.function.*;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;

public class SentimentAnalysis extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SentimentAnalysis.class);
    private int parserThreads;
    private int classifierThreads;

    public SentimentAnalysis(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(SentimentAnalysisConstants.Config.PARSER_THREADS, 1);
        classifierThreads = config.getInt(SentimentAnalysisConstants.Config.CLASSIFIER_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {
        Dataset<Row> rawRecords = createSource();

        Dataset<Row> records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSTweetParser(config), Encoders.kryo(Row.class));

        Dataset<Row> sentiments = records.filter(new SSFilterNull<>())
                .repartition(classifierThreads)
                .map(new SSCalculateSentiment(config), Encoders.kryo(Row.class));

        return createSink(sentiments);
    }

    @Override
    public String getConfigPrefix() {
        return SentimentAnalysisConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
