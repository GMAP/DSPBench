package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.constants.SpikeDetectionConstants;
import spark.streaming.model.Moving;
import spark.streaming.model.VisitStats;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author luandopke
 */
public class SSFlatRepeatVisit extends BaseFunction implements FlatMapGroupsWithStateFunction<String, Row, Boolean, Row> {

    public SSFlatRepeatVisit(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<Row> call(String key, Iterator<Row> values, GroupState<Boolean> state) throws Exception {
        List<Row> tuples = new ArrayList<>();
        Row tuple;
        while (values.hasNext()) {
            tuple = values.next();
            if (!state.exists()) {
                tuples.add(RowFactory.create(tuple.get(2), tuple.get(1), Boolean.TRUE));
                state.update(true);
            } else {
                tuples.add(RowFactory.create(tuple.get(2), tuple.get(1), Boolean.FALSE));
            }
        }
        return tuples.iterator();
    }
}