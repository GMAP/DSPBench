package spark.streaming.function;

import org.apache.spark.api.java.function.FlatMapFunction;
import spark.streaming.util.Configuration;

import java.util.Arrays;
import java.util.Iterator;

public class Split extends BaseFunction implements FlatMapFunction<String, String> {

    public Split(Configuration config) {
        super(config);
    }

    @Override
    public Iterator<String> call(String s) throws Exception {
        return Arrays.asList(s.split("\\W")).iterator();
    }
}