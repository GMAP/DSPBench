package com.streamer.examples.logprocessing;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import com.streamer.examples.utils.DateUtils;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class CommonLogParser extends Parser {
    private static final Logger LOG = LoggerFactory.getLogger(CommonLogParser.class);
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    
    public static final String IP        = "ip";
    public static final String TIMESTAMP = "timestamp";
    public static final String REQUEST   = "request";
    public static final String RESPONSE  = "response";
    public static final String BYTE_SIZE = "byte_size";
    private static final int NUM_FIELDS = 8;
    
    @Override
    public List<Values> parse(String str) {
        Map<String, Object> entry = parseLine(str);
        
        if (entry == null) {
            LOG.warn("Unable to parse log: {}", str);
            return null;
        }
        
        long minute = DateUtils.getMinuteForTime((Date) entry.get(TIMESTAMP));
        String id = String.format("%s:%s", entry.get(IP), entry.get(TIMESTAMP));
        
        Values values = new Values(entry.get(IP), minute, entry.get(RESPONSE),
                entry.get(TIMESTAMP), entry.get(REQUEST), entry.get(BYTE_SIZE));
        
        return ImmutableList.of(values);
    }
    
    public static Map<String, Object> parseLine(String logLine) {
        Map<String, Object> entry = new HashMap<String, Object>();
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";

        
        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logLine);
        
        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            return null;
        }
        
        entry.put(IP, matcher.group(1));
        entry.put(TIMESTAMP, dtFormatter.parseDateTime(matcher.group(4)).toDate());
        entry.put(REQUEST, matcher.group(5));
        entry.put(RESPONSE, Integer.parseInt(matcher.group(6)));
        
        if (matcher.group(7).equals("-"))
            entry.put(BYTE_SIZE, 0);
        else
            entry.put(BYTE_SIZE, Integer.parseInt(matcher.group(7)));
        
        return entry;
    }
}