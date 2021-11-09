package org.dspbench.applications.frauddetection;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang.StringUtils;
import static org.dspbench.constants.FraudDetectionConstants.*;

import org.dspbench.bolt.AbstractBolt;
import org.dspbench.applications.frauddetection.predictor.MarkovModelPredictor;
import org.dspbench.applications.frauddetection.predictor.ModelBasedPredictor;
import org.dspbench.applications.frauddetection.predictor.Prediction;

/**
 *
 * @author maycon
 */
public class FraudPredictorBolt extends AbstractBolt {
    private ModelBasedPredictor predictor;

    @Override
    public void initialize() {
        String strategy = config.getString(Conf.PREDICTOR_MODEL);

        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple input) {
        String entityID = input.getString(0);
        String record   = input.getString(1);
        Prediction p    = predictor.execute(entityID, record);

        // send outliers
        if (p.isOutlier()) {
            collector.emit(input, new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
        }
        
        collector.ack(input);
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.ENTITY_ID, Field.SCORE, Field.STATES);
    }
}