package spark.streaming.function;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import spark.streaming.util.Configuration;
import spark.streaming.util.Tuple;

public class Split extends BaseFunction implements FlatMapFunction<Tuple2<String, Tuple>, Tuple2<String, Tuple>> {

    public Split(Configuration config) {
        super(config);
    }

    @Override
    public Iterable<Tuple2<String, Tuple>> call(Tuple2<String, Tuple> input) {
        incReceived();

        List<Tuple2<String, Tuple>> words = new ArrayList<>();
        
        for (String word : input._1.split("\\W")) {
            if (!StringUtils.isBlank(word))
                words.add(new Tuple2<>(word, input._2));
        }
        
        incEmitted(words.size());
        
        return words;
    }
}