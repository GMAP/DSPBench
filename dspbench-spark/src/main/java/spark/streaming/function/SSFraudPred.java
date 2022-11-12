package spark.streaming.function;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
import spark.streaming.model.FraudRecord;
import spark.streaming.model.predictor.MarkovModelPredictor;
import spark.streaming.model.predictor.Prediction;
import spark.streaming.util.Configuration;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSFraudPred extends BaseFunction implements FlatMapGroupsWithStateFunction<String, Row, FraudRecord, Row> {

    public SSFraudPred(Configuration config) {
        super(config);
        predictor = new MarkovModelPredictor(getConfiguration());
    }

    private final MarkovModelPredictor predictor;
    private static Map<String, Long> throughput = new HashMap<>();
    ;
    private static BlockingQueue<String> queue = new ArrayBlockingQueue<>(20);;

    @Override
    public Iterator<Row> call(String entityID, Iterator<Row> values, GroupState<FraudRecord> state) throws Exception {
        Row tuple;
        List<Row> tuples = new ArrayList<>();
        String record;
        FraudRecord arr = null;
        Prediction p;
        while (values.hasNext()) {
            Calculate();
            tuple = values.next();
            record = tuple.getString(1);

            if (!state.exists()) {
                arr = new FraudRecord(record);
            } else {
                arr = state.get();
                arr.add(record);
            }

            p = predictor.execute(entityID, arr.getList());

            if (p.isOutlier()) {
                tuples.add(RowFactory.create(entityID, p.getScore(), StringUtils.join(p.getStates(), ","), tuple.get(tuple.size() - 1)));
            }
        }
        state.update(arr);
        return tuples.iterator();
    }

    public void Calculate() throws InterruptedException {
        Tuple2<Map<String, Long>, BlockingQueue<String>> d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 10) {
            super.SaveMetrics(queue.take());
        }
    }
}