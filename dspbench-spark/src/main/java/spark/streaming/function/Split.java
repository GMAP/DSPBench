package spark.streaming.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
public class Split extends BaseFunction implements FlatMapFunction<Row, Row> {

    public Split(Configuration config) {
        super(config);
    }

    @Override
    public void Calculate() throws InterruptedException {
        var d = super.calculateThroughput(throughput, queue);
        throughput = d._1;
        queue = d._2;
        if (queue.size() >= 10) {
            super.SaveMetrics(queue.take());
        }
    }

    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue= new ArrayBlockingQueue<>(20);

    @Override
    public Iterator<Row> call(Row s) throws Exception {
        Calculate();
        String[] words = s.getString(0).split("\\W");
        List<Row> tuples = new ArrayList<>();

        for (String word : words) {
            if (!StringUtils.isBlank(word))
                tuples.add(RowFactory.create(word, s.get(s.size() - 1)));
        }
        return tuples.iterator();
    }
}