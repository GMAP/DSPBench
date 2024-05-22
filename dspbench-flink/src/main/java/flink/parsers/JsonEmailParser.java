package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.JsonSyntaxException;

public class JsonEmailParser extends Parser implements MapFunction<String, Tuple3<String, String, Boolean>>{

    private static final Logger LOG = LoggerFactory.getLogger(JsonEmailParser.class);

    //private final Gson gson = new Gson();
    Configuration config;

    public JsonEmailParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple3<String, String, Boolean> map(String value) throws Exception {
        super.initialize(config);
        super.incReceived();
        try {
            //Don't know what makes an email a Spam
            JSONObject email = new JSONObject(value);

            super.incEmitted();
            return new Tuple3<String, String, Boolean>((String) email.get("id"), (String) email.get("message"), false);
        } catch (JsonSyntaxException ex) {
            LOG.error("Error parsing JSON encoded email", ex);
        }

        return null;
    }
    
    @Override
    public Tuple1<?> parse(String input) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'parse'");
    }
}
