package org.dspbench.spout.parser;

import org.dspbench.util.stream.StreamValues;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mayconbordin
 */
public class JsonTweetParser extends JsonParser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTweetParser.class);
    private static final String ID_FIELD   = "id";
    private static final String TEXT_FIELD = "text";
    private static final String DATE_FIELD = "created_at";
    private static final String DATA_FIELD = "data";
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    
    @Override
    public List<StreamValues> parse(String input) {
        List<StreamValues> tuples = super.parse(input);
        List<StreamValues> newValues = new ArrayList<StreamValues>();
        
        for (StreamValues values : tuples) {
            JSONObject tweet = (JSONObject) values.get(0);

            if (tweet.containsKey(DATA_FIELD)) {
                tweet = (JSONObject) tweet.get(DATA_FIELD);
            }
            
            if (!tweet.containsKey(ID_FIELD) || !tweet.containsKey(TEXT_FIELD) || !tweet.containsKey(DATE_FIELD))
                continue;

            String id = (String) tweet.get(ID_FIELD);
            String text = (String) tweet.get(TEXT_FIELD);
            DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get(DATE_FIELD));

            newValues.add(new StreamValues(id, text, timestamp.toDate()));
        }
        
        return newValues;
    }
}
