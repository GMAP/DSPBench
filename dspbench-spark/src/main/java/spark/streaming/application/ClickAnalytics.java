package spark.streaming.application;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.sql.streaming.OutputMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.ClickAnalyticsConstants;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.function.*;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;

import java.util.concurrent.TimeoutException;

public class ClickAnalytics extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(ClickAnalytics.class);
    private int parserThreads;
    private int repeatsThreads;
    private int geographyThreads;
    private int totalStatsThreads;
    private int geoStatsThreads;
    private int spoutThreads;
    private int visitSinkThreads;
    private int locationSinkThreads;


    public ClickAnalytics(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads =        config.getInt(ClickAnalyticsConstants.Config.PARSER_THREADS, 1);
        repeatsThreads       = config.getInt(ClickAnalyticsConstants.Config.REPEATS_THREADS, 1);
        geographyThreads     = config.getInt(ClickAnalyticsConstants.Config.GEOGRAPHY_THREADS, 1);
        totalStatsThreads    = config.getInt(ClickAnalyticsConstants.Config.TOTAL_STATS_THREADS, 1);
        geoStatsThreads      = config.getInt(ClickAnalyticsConstants.Config.GEO_STATS_THREADS, 1);

    }

    @Override
    public DataStreamWriter buildApplication() {
        var rawRecords = createSource();

        var records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new ClickStreamParser(config), Encoders.kryo(Row.class));

        var records2 = records
                .repartition(parserThreads)
                .map(new ClickStreamParser2(config), Encoders.kryo(Row.class));

        var records3 = records
                .repartition(parserThreads)
                .map(new ClickStreamParser3(config), Encoders.kryo(Row.class));
        try {
            var a =createSink(records3).start();
            var b = createSink(records2).start();

            a.awaitTermination();
            b.awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //testado com kafka, ele executa dois ao mesmo tempo porém le de novo do source como se fosse outra query, discutir isto com o dalvan quinta
        //três opçoes: se jogar pro spark streaming, aceitar esse comportamento de ler de novo do source ou fazer o processamento em batch;

//        var averages = records.filter(new SSFilterNull<>())
//                .repartition(movingAverageThreads)
//                .groupByKey((MapFunction<Row, Integer>) row -> row.getInt(0), Encoders.INT())
//                .flatMapGroupsWithState(new FlatMovingAverage(config), OutputMode.Update(), Encoders.kryo(Moving.class), Encoders.kryo(Row.class), GroupStateTimeout.NoTimeout());
//
//        var spikes = averages
//                .repartition(spikeDetectorThreads)
//                .map(new SpikeDetector(config), Encoders.kryo(Row.class))
//                .filter(new SSFilterNull<>());

        return createSink(records2);
    }

    @Override
    public String getConfigPrefix() {
        return ClickAnalyticsConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
