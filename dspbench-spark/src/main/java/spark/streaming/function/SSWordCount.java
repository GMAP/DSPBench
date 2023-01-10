package spark.streaming.function;

import org.apache.spark.api.java.function.MapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import scala.Tuple2;
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
      /*  Tuple2<Map<String, Long>, BlockingQueue<String>> d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 1) {
            super.SaveMetrics(queue.take());
        }*/
    }

    @Override
    public Row call(String key, Iterator<Row> values, GroupState<Long> state) throws Exception {
        long count = 0;
        while (values.hasNext()) {
            values.next();

            if (state.exists()) {
                count = state.get();
            }
            count++;
            state.update(count);

            Calculate();
            incBoth();
        }
        return RowFactory.create(key, count);
    }
}