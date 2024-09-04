package flink.application.machineoutiler;

import flink.application.AbstractApplication;
import flink.constants.MachineOutlierConstants;
import flink.parsers.AlibabaMachineUsageParser;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MachineOutlier extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(MachineOutlier.class);
    private int parserThreads;
    private int scorerThreads;
    private int anomalyScorerThreads;
    private int alertTriggerThreads;
    private int windowLength;

    public MachineOutlier(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        parserThreads = config.getInteger(MachineOutlierConstants.Conf.PARSER_THREADS, 1);
        scorerThreads = config.getInteger(MachineOutlierConstants.Conf.SCORER_THREADS, 1);
        anomalyScorerThreads = config.getInteger(MachineOutlierConstants.Conf.ANOMALY_SCORER_THREADS, 1);
        alertTriggerThreads = config.getInteger(MachineOutlierConstants.Conf.ALERT_TRIGGER_THREADS, 1);
        windowLength = config.getInteger(MachineOutlierConstants.Conf.ANOMALY_SCORER_WINDOW_LENGTH, 10);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> data = createSource();

        // Parser
        DataStream<Tuple3<String, Long, MachineMetadata>> dataParse = data
                .flatMap(new AlibabaMachineUsageParser(config)).setParallelism(parserThreads);

        // Process
        DataStream<Tuple4<String, Double, Long, Object>> scorer = dataParse.filter(value -> (value != null))
                .flatMap(new Scorer(config)).setParallelism(scorerThreads);

        DataStream<Tuple5<String, Double, Long, Object, Double>> anomaly = scorer.keyBy(value -> value.f0)
                .flatMap(new AnomalyScorer(config, windowLength)).setParallelism(anomalyScorerThreads);

        DataStream<Tuple5<String, Double, Long, Boolean, Object>> triggerer = anomaly
                .flatMap(new Triggerer(config)).setParallelism(alertTriggerThreads);

        // Sink
        createSinkMO(triggerer);

        return env;
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
