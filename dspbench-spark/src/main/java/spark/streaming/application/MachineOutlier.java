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
import spark.streaming.constants.MachineOutlierConstants;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.function.*;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;

public class MachineOutlier extends AbstractApplication {
    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlier.class);
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;
    private int parserThreads;

    public MachineOutlier(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInt(ClickAnalyticsConstants.Config.PARSER_THREADS, 1);
        scorerThreads = config.getInt(MachineOutlierConstants.Config.SCORER_THREADS, 1);
        anomalyScorerThreads = config.getInt(MachineOutlierConstants.Config.ANOMALY_SCORER_THREADS, 1);
        alertTriggerThreads = config.getInt(MachineOutlierConstants.Config.ALERT_TRIGGER_THREADS, 1);
    }

    @Override
    public DataStreamWriter buildApplication() {
        var rawRecords = createSource();

        var records = rawRecords
                .repartition(parserThreads)
                .as(Encoders.STRING())
                .map(new SSAlibabaMachineUsageParser(config), Encoders.kryo(Row.class));

        var score = records.filter(new SSFilterNull<>())
                .repartition(scorerThreads)
                .flatMap(new SSObservationScore(config), Encoders.kryo(Row.class));

        var anomaly = score.filter(new SSFilterNull<>())
                .repartition(anomalyScorerThreads)
                .map(new SSSlidingWindowStreamAnomalyScore(config), Encoders.kryo(Row.class));

        var alert = anomaly.filter(new SSFilterNull<>())
                .repartition(alertTriggerThreads)
                .flatMap(new SSAlertTrigger(config), Encoders.kryo(Row.class));

        return createSink(alert);
    }

    @Override
    public String getConfigPrefix() {
        return MachineOutlierConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
