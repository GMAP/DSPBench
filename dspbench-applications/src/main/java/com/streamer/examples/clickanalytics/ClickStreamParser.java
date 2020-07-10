package com.streamer.examples.clickanalytics;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import com.streamer.utils.HashUtils;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class ClickStreamParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(ClickStreamParser.class);
    
    private final Gson gson = new Gson();

    @Override
    public List<Values> parse(String input) {
        Values values = null;
        
        try {
            ClickStream clickstream = gson.fromJson(input, ClickStream.class);
            values = new Values(clickstream.ip, clickstream.url, clickstream.clientKey);
        } catch (JsonSyntaxException ex) {
            LOG.error("Error parsing JSON encoded clickstream: " + input, ex);
        }
        
        return ImmutableList.of(values);
    }
    
    private static class ClickStream {
        public String ip;
        public String url;
        public String clientKey;
    }
}