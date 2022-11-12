package spark.streaming.function;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.FraudDetectionConstants;
import spark.streaming.model.predictor.MarkovModelPredictor;
import spark.streaming.model.predictor.ModelBasedPredictor;
import spark.streaming.model.predictor.Prediction;
import spark.streaming.util.Configuration;

/**
 * @author luandopke
 */
public class SSFraudPredictor extends BaseFunction implements MapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSFraudPredictor.class);
    private ModelBasedPredictor predictor;

    public SSFraudPredictor(Configuration config) {
        super(config);
        String strategy = config.get(FraudDetectionConstants.Config.PREDICTOR_MODEL);
        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public Row call(Row value) throws Exception {
        super.calculateThroughput();
        String entityID = value.getString(0);
        String record = value.getString(1);
//        Prediction p = predictor.execute(entityID, record);
//
//        // send outliers
//        if (p.isOutlier()) {
//            return RowFactory.create(entityID, p.getScore(), StringUtils.join(p.getStates(), ","), value.get(value.size() - 1));
//        }
        return null;
    }
}