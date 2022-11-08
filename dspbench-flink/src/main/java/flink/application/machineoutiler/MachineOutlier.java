package flink.application.machineoutiler;

import flink.application.AbstractApplication;
import flink.application.logprocessing.GeoFinder;
import flink.application.logprocessing.GeoStats;
import flink.application.logprocessing.StatusCount;
import flink.application.logprocessing.VolumeCount;
import flink.constants.LogProcessingConstants;
import flink.constants.MachineOutlierConstants;
import flink.parsers.AlibabaMachineUsageParser;
import flink.parsers.CommonLogParser;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MachineOutlier extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlier.class);
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;

    public MachineOutlier(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        scorerThreads        = config.getInteger(MachineOutlierConstants.Conf.SCORER_THREADS, 1);
        anomalyScorerThreads = config.getInteger(MachineOutlierConstants.Conf.ANOMALY_SCORER_THREADS, 1);
        alertTriggerThreads  = config.getInteger(MachineOutlierConstants.Conf.ALERT_TRIGGER_THREADS, 1);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple4<String, Long, MachineMetadata, String>> dataParse = data.map(new AlibabaMachineUsageParser(config));

        // Process
        DataStream<Tuple5<String, Double, Long, Object, String>> scorer = dataParse.filter(value -> (value != null)).flatMap(new Scorer(config));

        DataStream<Tuple6<String, Double, Long, Object, Double, String>> anomaly = scorer.keyBy(value -> value.f0).flatMap(new AnomalyScorer(config));

        DataStream<Tuple6<String, Double, Long, Boolean, Object, String>> triggerer = anomaly.flatMap(new Triggerer(config));

        // Sink
        createSinkMO(triggerer);

        return env;
    }

    @Override
    public String getConfigPrefix() { return MachineOutlierConstants.PREFIX; }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
