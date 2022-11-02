package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.model.Moving;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author luandopke
 */
public class SSFlatMovingAverage extends BaseFunction implements FlatMapGroupsWithStateFunction<Integer, Row, Moving, Row> {
    private int movingAverageWindow;

    public SSFlatMovingAverage(Configuration config) {
        super(config);
        movingAverageWindow = config.getInt(SpikeDetectionConstants.Config.MOVING_AVERAGE_WINDOW, 1000);
    }

    @Override
    public Iterator<Row> call(Integer key, Iterator<Row> values, GroupState<Moving> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        double value = 0, avg = 0;
        Moving mov;
        while (values.hasNext()) {
            value = values.next().getDouble(2);
            avg = value;

            if (!state.exists()) {
                mov = new Moving(key);
                mov.add(value);
            } else {
                mov = state.get();

                if (mov.getList().size() > movingAverageWindow - 1) {
                    mov.remove();
                }
                mov.add(value);
                avg = mov.getSum() / mov.getList().size();
            }

            state.update(mov);
            tuples.add(RowFactory.create(key, avg, value));
        }
        return tuples.iterator();
    }
}