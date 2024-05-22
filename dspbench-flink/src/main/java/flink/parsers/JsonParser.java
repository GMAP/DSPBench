package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JsonParser extends Parser implements MapFunction<String, Tuple1<JSONObject>> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    private static final JSONParser jsonParser = new JSONParser();

    Configuration config;

    public JsonParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }

    @Override
    public Tuple1<JSONObject> map(String value) throws Exception {
        super.initialize(config);
        value = value.trim();
        super.incReceived();
        if (value.isEmpty() || (!value.startsWith("{") && !value.startsWith("[")))
            return null;
        
        try {
            super.incEmitted();
            JSONObject json = (JSONObject) jsonParser.parse(value);
            return new Tuple1<JSONObject>((JSONObject) json.get("data"));
        } catch (ParseException e) {
            LOG.error(String.format("Error parsing JSON object: %s", value), e);
        }
        
        return null;
    }
}