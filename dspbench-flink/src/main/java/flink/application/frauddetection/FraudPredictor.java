package flink.application.frauddetection;

import flink.application.frauddetection.predictor.MarkovModelPredictor;
import flink.application.frauddetection.predictor.ModelBasedPredictor;
import flink.application.frauddetection.predictor.Prediction;
import flink.constants.FraudDetectionConstants;
import flink.util.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudPredictor extends Metrics
        implements FlatMapFunction<Tuple2<String, String>, Tuple3<String, Double, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudPredictor.class);

    private ModelBasedPredictor predictor;

    Configuration config;

    public FraudPredictor(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    private ModelBasedPredictor createPred() {
        String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL, "mm");
        if (predictor == null && strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }

        return predictor;
    }

    @Override
    public void flatMap(Tuple2<String, String> input, Collector<Tuple3<String, Double, String>> out) {
        super.initialize(config);
        createPred();
        super.incReceived();
        String entityID = input.getField(0);
        String record = input.getField(1);

        Prediction p = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            super.incEmitted();
            out.collect(new Tuple3<>(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
        }
    }
}
