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
public class SSWordCount extends BaseFunction implements MapGroupsWithStateFunction<String, String, Long, Row> {

    public SSWordCount(Configuration config) {
        super(config);
    }

    @Override
    public Row call(String key, Iterator<String> values, GroupState<Long> state) throws Exception {
        long count =0;
        while (values.hasNext()) {
            if (state.exists()) {
                count = state.get();
            }
            count++;
            state.update(count);
            values.next();
        }
        return RowFactory.create(key, count);
    }
}