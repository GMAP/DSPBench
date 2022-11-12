package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.streaming.GroupState;
import spark.streaming.model.VisitStats;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * @author luandopke
 */
public class SSVisitStats extends BaseFunction implements FlatMapGroupsWithStateFunction<Integer, Row, VisitStats, Row> {

    public SSVisitStats(Configuration config) {
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
    public Iterator<Row> call(Integer key, Iterator<Row> values, GroupState<VisitStats> state) throws Exception {
        VisitStats stats;
        Row tuple;
        List<Row> tuples = new ArrayList<>();
        while (values.hasNext()) {
            Calculate();
            tuple = values.next();
            if (!state.exists()) {
                stats = new VisitStats();
            } else {
                stats = state.get();
            }
            stats.add(tuple.getBoolean(2));
            state.update(stats);
            tuples.add(RowFactory.create(stats.getTotal(), stats.getUniqueCount(), tuple.get(tuple.size() - 1)));
        }
        return tuples.iterator();
    }
}