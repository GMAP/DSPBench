package org.dspbench.applications.clickanalytics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dspbench.base.source.parser.Parser;
import org.dspbench.core.Values;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ClickStreamParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);
    
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public List<Values> parse(String input) {
        try {
            ClickStream clickstream = objectMapper.readValue(input, ClickStream.class);
            Values values = new Values(clickstream.ip, clickstream.url, clickstream.clientKey);
            return List.of(values);
        } catch (JsonProcessingException ex) {
            LOG.error("Error parsing JSON encoded clickstream: " + input, ex);
        }
        
        return List.of();
    }
    
    private static class ClickStream {
        public String ip;
        public String url;
        public String clientKey;
    }
}