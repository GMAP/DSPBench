package spark.streaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.Trigger;
import spark.streaming.constants.BaseConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ConsoleSink extends BaseSink {
    private static BlockingQueue<String> queue = new ArrayBlockingQueue<>(20);
    private static Map<String, Long> throughput = new HashMap<>();

    private static BlockingQueue<String> queue2 = new ArrayBlockingQueue<>(20);
    private static Map<String, Long> throughput2 = new HashMap<>();

    private static BlockingQueue<String> queue3 = new ArrayBlockingQueue<>(20);
    private static Map<String, Long> throughput3 = new HashMap<>();

    @Override
    public void Calculate(int sink) throws InterruptedException, RuntimeException {
        switch (sink) {
            case 1: {
                var d = super.calculateThroughput(throughput, queue);
                throughput = d._1;
                queue = d._2;
                if (queue.size() >= 10) {
                    super.SaveMetrics(queue.take());
                }
                break;
            }
            case 2: {
                var d = super.calculateThroughput(throughput2, queue2);
                throughput2 = d._1;
                queue2 = d._2;
                if (queue2.size() >= 10) {
                    super.SaveMetrics(queue2.take());
                }
                break;
            }
            case 3: {
                var d = super.calculateThroughput(throughput3, queue3);
                throughput3 = d._1;
                queue3 = d._2;
                if (queue3.size() >= 10) {
                    super.SaveMetrics(queue3.take());
                }
                break;
            }
        }
    }

    @Override
    public DataStreamWriter<Row> sinkStream(Dataset<Row> dt, int sink) {
        return dt.writeStream().foreach(new ForeachWriter<Row>() {

                    @Override
                    public boolean open(long partitionId, long version) {
                        return true;
                    }

                    @Override
                    public void process(Row value) {
                        System.out.println(value);

                        try {
                            Calculate(sink);
                            if (value != null)
                                calculateLatency(value.getLong(value.size() - 1));
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    } //TODO make formater as field=value,

                    @Override
                    public void close(Throwable errorOrNull) {
                        // Close the connection
                    }
                }).outputMode(config.get(BaseConstants.BaseConfig.OUTPUT_MODE, "update"))
                .trigger(Trigger.AvailableNow());
    }
}
