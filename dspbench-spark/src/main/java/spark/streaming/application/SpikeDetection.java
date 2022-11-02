package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.function.*;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;

public class SpikeDetection extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(SpikeDetection.class);
    private int parserThreads;
    private int movingAverageThreads;
    private int spikeDetectorThreads;

    public SpikeDetection(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(SpikeDetectionConstants.Config.PARSER_THREADS, 1);
        movingAverageThreads = config.getInt(SpikeDetectionConstants.Config.MOVING_AVERAGE_THREADS, 1);
        spikeDetectorThreads = config.getInt(SpikeDetectionConstants.Config.SPIKE_DETECTOR_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {
        var rawRecords = createSource();

        var records = rawRecords
                .repartition(parserThreads)//TODO: change to coalesce
                .as(Encoders.STRING())
                .map(new SSSensorParser(config), Encoders.kryo(Row.class));

        var averages = records.filter(new SSFilterNull<>())
                .repartition(movingAverageThreads)
                .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0), Encoders.INT())
                .flatMapGroupsWithState(new SSFlatMovingAverage(config), OutputMode.Update(), Encoders.kryo(Moving.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());

        var spikes = averages
                .repartition(spikeDetectorThreads)
                .map(new SSSpikeDetector(config), Encoders.kryo(Row.class))
                .filter(new SSFilterNull<>());

        return createSink(spikes);
    }

    @Override
    public String getConfigPrefix() {
        return SpikeDetectionConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
