package spark.streaming.sink;

import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import spark.streaming.function.BaseFunction;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class Sink<K> extends BaseFunction implements Function<JavaRDD<Tuple2<K, Tuple>>, Void> {
    private Timer latencyTimer;

    public Sink(Configuration config) {
        super(config);
    }

    public Sink(Configuration config, String name) {
        super(config, name);
    }

    @Override
    public Void call(JavaRDD<Tuple2<K, Tuple>> rdd) throws Exception {
        incReceived(rdd.count());
        
        final long now = System.currentTimeMillis();
        
        rdd.foreach(new VoidFunction<Tuple2<K, Tuple>>() {
            @Override
            public void call(Tuple2<K, Tuple> t) throws Exception {
                long start = t._2.getCreatedAt();
                getLatencyTimer().update(now-start, TimeUnit.MILLISECONDS);
            }
        });

        return null;
    }
    
    protected Timer getLatencyTimer() {
        if (latencyTimer == null) {
            latencyTimer = getMetrics().timer(String.format("%s-%d.tuple-latency", getName(), getId()));
        }
        return latencyTimer;
    }
    
}
