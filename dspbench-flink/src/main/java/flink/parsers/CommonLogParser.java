package flink.parsers;

import flink.application.logprocessing.DateUtils;
import flink.util.Configurations;
import flink.util.Metrics;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CommonLogParser extends RichFlatMapFunction<String, Tuple6<Object, Object, Long, Object, Object, Object>> {

    private static final Logger LOG = LoggerFactory.getLogger(CommonLogParser.class);

    Configuration config;

    Metrics metrics = new Metrics();

    public CommonLogParser(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(String input, Collector<Tuple6<Object, Object, Long, Object, Object, Object>> out)
            throws Exception {
                metrics.initialize(config, this.getClass().getSimpleName());
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.receiveThroughput();
                }
        
                Map<String, Object> entry = parseLine(input);
        
                if (entry == null) {
                    System.out.println("Unable to parse log: " + input);
                    out.collect(new Tuple6<>(null, null, null, null, null, null));
                }
        
                if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                    metrics.emittedThroughput();
                }
                
                long minute = DateUtils.getMinuteForTime((Date) entry.get("TIMESTAMP"));
                out.collect(new Tuple6<Object, Object, Long, Object, Object, Object>( entry.get("IP"), entry.get("TIMESTAMP"), minute, entry.get("REQUEST"), entry.get("RESPONSE"), entry.get("BYTE_SIZE")));
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
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

