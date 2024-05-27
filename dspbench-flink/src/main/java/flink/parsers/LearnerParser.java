package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LearnerParser extends Parser implements MapFunction<String, Tuple2<String, Integer>> {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerParser.class);

    Configuration config;

    public LearnerParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple2<String, Integer> map(String value) throws Exception {
        super.initialize(config);
        super.incBoth();
        String[] temp = value.split(",");
        if(temp[0] == null || temp[1] == null){
            return null;
        }
        else{
            return new Tuple2<String, Integer>(temp[0], Integer.parseInt(temp[1]));
        }
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
