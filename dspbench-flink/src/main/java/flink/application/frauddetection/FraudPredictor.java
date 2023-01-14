package flink.application.frauddetection;

import flink.application.frauddetection.predictor.MarkovModelPredictor;
import flink.application.frauddetection.predictor.ModelBasedPredictor;
import flink.application.frauddetection.predictor.Prediction;
import flink.constants.FraudDetectionConstants;
import flink.util.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudPredictor extends Metrics implements FlatMapFunction<Tuple3<String, String, String>, Tuple4<String, Double, String,String>> {

    private static final Logger LOG = LoggerFactory.getLogger(FraudPredictor.class);

    private ModelBasedPredictor predictor;

    Configuration config;

    public FraudPredictor(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    private ModelBasedPredictor createPred(){
        String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL, "mm");
        if (predictor == null && strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }

        return predictor;
    }

    @Override
    public void flatMap(Tuple3<String, String, String> input, Collector<Tuple4<String, Double, String, String>> out){
        super.initialize(config);
        createPred();
        String entityID = input.getField(0);
        String record = input.getField(1);
        String initTime = input.getField(2);

        Prediction p = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            out.collect(new Tuple4<>(entityID, p.getScore(), StringUtils.join(p.getStates(), ","), initTime));
        }
        super.calculateThroughput();
    }
}
