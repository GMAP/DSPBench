package org.dspbench.applications.frauddetection;

import org.dspbench.base.operator.BaseOperator;
import org.dspbench.core.Tuple;
import org.dspbench.core.Values;
import org.dspbench.applications.frauddetection.predictor.MarkovModelPredictor;
import org.dspbench.applications.frauddetection.predictor.ModelBasedPredictor;
import org.dspbench.applications.frauddetection.predictor.Prediction;
import org.apache.commons.lang3.StringUtils;

public class PredictorOperator extends BaseOperator {
    private ModelBasedPredictor predictor;

    @Override
    public void initialize() {
        String strategy = config.getString(FraudDetectionConstants.Config.PREDICTOR_MODEL, FraudDetectionConstants.PredictorModel.MARKOV_MODEL);
        
        if (strategy.equals(FraudDetectionConstants.PredictorModel.MARKOV_MODEL)) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    public void process(Tuple input) {
        String entityID = input.getString(FraudDetectionConstants.Field.ENTITY_ID);
        String record   = input.getString(FraudDetectionConstants.Field.RECORD_DATA);
        Prediction p    = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
        }
    }
}