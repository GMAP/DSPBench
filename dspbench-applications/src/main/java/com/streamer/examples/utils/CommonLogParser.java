package com.streamer.examples.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class CommonLogParser {
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z");
    
    public static final String IP        = "ip";
    public static final String TIMESTAMP = "timestamp";
    public static final String REQUEST   = "request";
    public static final String RESPONSE  = "response";
    public static final String BYTE_SIZE = "byte_size";
    
    public static Map<String, Object> parse(String logLine) {
        Map<String, Object> entry = new HashMap<String, Object>();
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";

        int NUM_FIELDS = 8;
        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logLine);
        
        if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
            return null;
        }
        
        entry.put("ip", matcher.group(1));
        entry.put("timestamp", dtFormatter.parseDateTime(matcher.group(4)).toDate());
        entry.put("request", matcher.group(5));
        entry.put("response", Integer.parseInt(matcher.group(6)));
        
        if (matcher.group(7).equals("-"))
            entry.put("byte_size", 0);
        else
            entry.put("byte_size", Integer.parseInt(matcher.group(7)));
        
        return entry;
    }
}
