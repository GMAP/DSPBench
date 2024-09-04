package flink.application.machineoutiler;

import flink.constants.MachineOutlierConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Scorer extends RichFlatMapFunction<Tuple3<String, Long, MachineMetadata>, Tuple4<String, Double, Long, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(Scorer.class);

    private static long previousTimestamp;
    private static String dataTypeName;
    private static List<Object> observationList;
    private static DataInstanceScorer dataInstanceScorer;
    Configuration config;
    Metrics metrics = new Metrics();

    public Scorer(Configuration config) {
        metrics.initialize(config, this.getClass().getSimpleName());
        previousTimestamp = 0;
        this.config = config;
    }

    private List<Object> getList() {
        if (observationList == null) {
            observationList = new ArrayList<>();
        }

        return observationList;
    }

    private DataInstanceScorer createScorer(Configuration config) {
        if (dataInstanceScorer == null) {
            dataTypeName = config.getString(MachineOutlierConstants.Conf.SCORER_DATA_TYPE, "machineMetadata");
            dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
        }

        return dataInstanceScorer;
    }

    @Override
    public void flatMap(Tuple3<String, Long, MachineMetadata> input,
            Collector<Tuple4<String, Double, Long, Object>> out) {
        metrics.initialize(config, this.getClass().getSimpleName());
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        getList();
        createScorer(config);

        long timestamp = input.getField(1);
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                        metrics.emittedThroughput();
                    }
            
                    out.collect(new Tuple4<String, Double, Long, Object>(scorePackage.getId(),
                            scorePackage.getScore(), previousTimestamp, scorePackage.getObj()));
                }
                observationList.clear();
            }

            previousTimestamp = timestamp;
        }
        observationList.add(input.getField(2));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}
