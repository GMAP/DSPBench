package spark.streaming.sink;

import com.codahale.metrics.Timer;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import spark.streaming.function.BaseFunction;
import spark.streaming.metrics.Latency;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class PairSink<K> extends BaseFunction implements VoidFunction<JavaPairRDD<K, Tuple>> {
    //private transient Timer latencyTimer;
    private transient Latency latencyTimer;

    public PairSink(Configuration config) {
        super(config);
    }

    public PairSink(Configuration config, String name) {
        super(config, name);
    }

    @Override
    public void call(JavaPairRDD<K, Tuple> rdd) throws Exception {
        incReceived(rdd.count());
        
        final long now = System.currentTimeMillis();
        
        rdd.foreach(new VoidFunction<Tuple2<K, Tuple>>() {
            @Override
            public void call(Tuple2<K, Tuple> t) throws Exception {
                long start = t._2.getCreatedAt();
                getLatencyTimer().update(now-start, TimeUnit.MILLISECONDS);
            }
        });
    }
    
    protected Latency getLatencyTimer() {
        if (latencyTimer == null) {
            latencyTimer = getMetrics().register(String.format("%s-%d.tuple-latency", getName(), getId()), new Latency());
        }

        return latencyTimer;
    }
    
    /*protected Timer getLatencyTimer() {
        if (latencyTimer == null) {
            latencyTimer = getMetrics().timer(String.format("%s-%d.tuple-latency", getName(), getId()));
        }

        return latencyTimer;
    }*/
    
}
