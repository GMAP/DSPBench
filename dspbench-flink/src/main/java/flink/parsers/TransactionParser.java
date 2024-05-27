package flink.parsers;

import flink.util.Metrics;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 *
 */
public class TransactionParser extends Metrics implements MapFunction<String, Tuple2<String, String>> {

    Configuration config;

    public TransactionParser(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple2<String, String> map(String value) throws Exception {
        super.initialize(config);
        super.incBoth();
        String[] temp = value.split(",", 2);
        if(temp[0] == null || temp[1] == null ){
            return null;
        }else{
            return new Tuple2<>(
                temp[0],
                temp[1]);
        }
    }

}
