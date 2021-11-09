package spark.streaming.function;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

/**
 *
 * @author mayconbordin
 */
public class KafkaParser extends BaseFunction implements Function<Tuple2<String, String>, Tuple2<String, Tuple>> {

    public KafkaParser(Configuration config) {
        super(config);
    }

    @Override
    public Tuple2<String, Tuple> call(Tuple2<String, String> t1) throws Exception {
        incEmitted();
        return new Tuple2<>(t1._2(), new Tuple());
    }
    
}
