package SentimentAnalysis;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.java.tuple.Tuple1;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class JsonParser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    private static final JSONParser jsonParser = new JSONParser();

    public Tuple1<JSONObject> parse(String input) {
        input = input.trim();
        
        if (input.isEmpty() || (!input.startsWith("{") && !input.startsWith("[")))
            return null;
        
        try {
            JSONObject json = (JSONObject) jsonParser.parse(input);
            return new Tuple1<JSONObject>(json);
        } catch (ParseException e) {
            LOG.error(String.format("Error parsing JSON object: %s", input), e);
        }
        
        return null;
    }
}