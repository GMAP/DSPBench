package spark.streaming.function;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.util.Configuration;

import java.util.Iterator;

/**
 * @author luandopke
 */
public class SSWordCount extends BaseFunction implements MapGroupsWithStateFunction<String, Row, Long, Row> {

    public SSWordCount(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String key, Iterator<Row> values, GroupState<Long> state) throws Exception {
        long count = 0, inittime = 0;
        while (values.hasNext()) {
            var tuple = values.next();
            inittime = tuple.getLong(tuple.size() - 1);

            if (state.exists()) {
                count = state.get();
            }
            count++;
            state.update(count);

            super.calculateThroughput();
        }
        return RowFactory.create(key, count, inittime);
    }
}