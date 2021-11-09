package com.streamer.examples.sentimentanalysis;

import com.streamer.base.source.parser.JsonParser;
import com.streamer.core.Values;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class JsonTweetParser extends JsonParser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonTweetParser.class);
    private static final String ID_FIELD   = "id_str";
    private static final String TEXT_FIELD = "text";
    private static final String DATE_FIELD = "created_at";
    private static final DateTimeFormatter datetimeFormatter = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy")
                                                                             .withLocale(Locale.ENGLISH);
    
    @Override
    public List<Values> parse(String input) {
        List<Values> tuples = super.parse(input);
        List<Values> newValues = new ArrayList<Values>();
        
        for (Values values : tuples) {
            JSONObject tweet = (JSONObject) values.get(0);
            
            if (!tweet.containsKey(ID_FIELD) || !tweet.containsKey(TEXT_FIELD) || !tweet.containsKey(DATE_FIELD))
                continue;

            String id = (String) tweet.get(ID_FIELD);
            String text = (String) tweet.get(TEXT_FIELD);
            DateTime timestamp = datetimeFormatter.parseDateTime((String) tweet.get(DATE_FIELD));

            newValues.add(new Values(id, text, timestamp.toDate()));
        }
        
        return newValues;
    }
}
