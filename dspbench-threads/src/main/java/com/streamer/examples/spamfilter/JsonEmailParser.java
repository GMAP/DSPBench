package com.streamer.examples.spamfilter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class JsonEmailParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(JsonEmailParser.class);
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<Values> parse(String str) {
        Values values = null;
        
        try {
            Email email = objectMapper.readValue(str, Email.class);
            
            values = new Values();
            values.add(email.id);
            values.add(email.message);
            values.setId(email.id.hashCode());
            
            if (email.isSpam != null) {
                values.add(email.isSpam);
            }
        } catch (JsonProcessingException ex) {
            LOG.error("Error parsing JSON encoded email", ex);
        }
        
        return List.of(values);
    }
    
    private static class Email {
        public String id;
        public String message;
        public Boolean isSpam = null;
    }
}