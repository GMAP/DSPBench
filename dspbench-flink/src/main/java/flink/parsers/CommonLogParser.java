package flink.parsers;

import flink.application.logprocessing.DateUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonLogParser extends Parser implements MapFunction<String, Tuple7<Object, Object, Long, Object, Object, Object, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonLogParser.class);

    Configuration config;

    public CommonLogParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple7<Object, Object, Long, Object, Object, Object, String> map(String value) throws Exception {
        super.initialize(config);
        super.calculateThroughput();

        Map<String, Object> entry = parseLine(value);

        if (entry == null) {
            System.out.println("Unable to parse log: " + value);
            return null;
        }

        long minute = DateUtils.getMinuteForTime((Date) entry.get("TIMESTAMP"));
        return new Tuple7<Object, Object, Long, Object, Object, Object, String>( entry.get("IP"), entry.get("TIMESTAMP"), minute, entry.get("REQUEST"), entry.get("RESPONSE"), entry.get("BYTE_SIZE"), Instant.now().toEpochMilli() + "");
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }

    Map<String, Object> parseLine(String logLine) {
        Map<String, Object> entry = new HashMap<>();
        String logEntryPattern = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\S+)(.*?)";


        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logLine);

        if (!matcher.matches() || 8 != matcher.groupCount()) {
            return null;
        }

        entry.put("IP", matcher.group(1));
        entry.put("TIMESTAMP", DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z").parseDateTime(matcher.group(4)).toDate());
        entry.put("REQUEST", matcher.group(5));
        entry.put("RESPONSE", Integer.parseInt(matcher.group(6)));

        if (matcher.group(7).equals("-"))
            entry.put("BYTE_SIZE", 0);
        else
            entry.put("BYTE_SIZE", Integer.parseInt(matcher.group(7)));

        return entry;
    }
}

