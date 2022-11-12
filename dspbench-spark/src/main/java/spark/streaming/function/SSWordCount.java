package spark.streaming.function;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.util.Configuration;

import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSWordCount extends BaseFunction implements MapGroupsWithStateFunction<String, Row, Long, Row> {

    public SSWordCount(Configuration config) {
        super(config);
    }

    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue = new ArrayBlockingQueue<>(20);

    @Override
    public void Calculate() throws InterruptedException {
        var d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 10) {
            super.SaveMetrics(queue.take());
        }
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

            Calculate();
        }
        return RowFactory.create(key, count, inittime);
    }
}