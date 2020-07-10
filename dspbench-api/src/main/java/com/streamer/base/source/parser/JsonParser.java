package com.streamer.base.source.parser;

import com.google.common.collect.ImmutableList;
import com.streamer.core.Values;
import java.util.List;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class JsonParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonParser.class);
    private static final JSONParser jsonParser = new JSONParser();
    private long count = 0;
    
    public List<Values> parse(String input) {
        input = input.trim();
        
        if (input.isEmpty() || (!input.startsWith("{") && !input.startsWith("[")))
            return null;
        
        try {
            JSONObject json = (JSONObject) jsonParser.parse(input);
            Values values = new Values(json);
            values.setId(count++);
            
            return ImmutableList.of(values);
        } catch (ParseException e) {
            LOG.error(String.format("Error parsing JSON object: %s", input), e);
        }
        
        return null;
    }
}