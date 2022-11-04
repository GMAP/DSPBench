package flink.parsers;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class ClickStreamParser extends Parser implements MapFunction<String, Tuple4<String, String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    @Override
    public Tuple4<String, String, String, String> map(String value) throws Exception {
        super.calculateThroughput();
        Gson gson = new Gson();
        try {
            ClickStream clickstream = gson.fromJson(value, ClickStream.class);
            return new Tuple4<String, String, String, String>(clickstream.ip, clickstream.url, clickstream.clientKey, Instant.now().toEpochMilli() + "");
        } catch (Exception ex) {
            System.out.println("Error parsing JSON encoded clickStream: " + value + " - " + ex);
        }
        return null;
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
}

class ClickStream {
    public String ip;
    public String url;
    public String clientKey;
}
