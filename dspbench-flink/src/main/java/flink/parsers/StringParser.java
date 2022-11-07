package flink.parsers;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;

public class StringParser extends Parser implements MapFunction<String, Tuple2<String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(StringParser.class);

    Configuration config;

    public StringParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple2<String, String> map(String value) throws Exception {
        super.initialize(config);
        super.calculateThroughput();
        if (StringUtils.isBlank(value))
            return null;

        return new Tuple2<String,String>(value,  Instant.now().toEpochMilli() + "");
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}
