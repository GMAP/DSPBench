package com.streamer.examples.frauddetection;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.frauddetection.FraudDetectionConstants.Config;
import com.streamer.examples.frauddetection.FraudDetectionConstants.Field;
import static com.streamer.examples.frauddetection.FraudDetectionConstants.PredictorModel.MARKOV_MODEL;
import com.streamer.examples.frauddetection.predictor.MarkovModelPredictor;
import com.streamer.examples.frauddetection.predictor.ModelBasedPredictor;
import com.streamer.examples.frauddetection.predictor.Prediction;
import org.apache.commons.lang3.StringUtils;

public class PredictorOperator extends BaseOperator {
    private ModelBasedPredictor predictor;

    @Override
    public void initialize() {
        String strategy = config.getString(Config.PREDICTOR_MODEL, MARKOV_MODEL);
        
        if (strategy.equals(MARKOV_MODEL)) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    public void process(Tuple input) {
        String entityID = input.getString(Field.ENTITY_ID);
        String record   = input.getString(Field.RECORD_DATA);
        Prediction p    = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
        }
    }
}