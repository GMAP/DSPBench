package flink.parsers;

import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickStreamParser extends Parser implements MapFunction<String, Tuple3<String, String, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);

    Configuration config;

    public ClickStreamParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple3<String, String, String> map(String value) throws Exception {
        super.initialize(config);
        super.incBoth();
        Gson gson = new Gson();
        try {
            ClickStream clickstream = gson.fromJson(value, ClickStream.class);
            return new Tuple3<>(clickstream.ip, clickstream.url, clickstream.clientKey);
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
