package spark.streaming.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public abstract class BaseSource {
    protected Configuration config;
    protected JavaStreamingContext context;
    
    public void initialize(Configuration config, JavaStreamingContext context, String prefix) {
        this.config = config;
        this.context = context;
    }
    
    public abstract JavaDStream<Tuple2<String, Tuple>> createStream();
}
