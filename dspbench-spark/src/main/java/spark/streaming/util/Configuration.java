package spark.streaming.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import scala.Tuple2;

/**
 * @author mayconbordin
 */
public class Configuration extends SparkConf implements Serializable {
    public static final String GEOIP_INSTANCE = "geoip.instance";
    public static final String GEOIP2_DB = "geoip2.db";

    public static final String METRICS_ENABLED = "metrics.enabled";
    public static final String METRICS_ENABLED_DEFAULT = "false";

    public static final String METRICS_INTERVAL = "metrics.interval";
    public static final String METRICS_INTERVAL_DEFAULT = "5";

    public static final String METRICS_PROCESS_TIME_SAMPLE_RATE = "metrics.process_time.sample_rate";
    public static final String METRICS_PROCESS_TIME_SAMPLE_RATE_DEFAULT = "0.05";

    public static final String METRICS_MEMORY_ENABLED = "metrics.memory.enabled";
    public static final String METRICS_MEMORY_ENABLED_DEFAULT = "false";

    public static final String METRICS_REPORTER = "metrics.reporter";
    public static final String METRICS_OUTPUT = "metrics.output";

    public static final String METRICS_STATSD_HOST = "metrics.statsd.host";
    public static final String METRICS_STATSD_HOST_DEFAULT = "localhost";
    public static final String METRICS_STATSD_PORT = "metrics.statsd.port";
    public static final String METRICS_STATSD_PORT_DEFAULT = "8125";

    public static final String METRICS_ENABLED_METRICS = "metrics.enabled.metrics";
    public static final String METRICS_ENABLED_METRICS_DEFAULT = "throughput,tuple-counter,tuple-latency-hdr";

    public static final String HDR_HIGHEST_TRACKABLE_VALUE = "metrics.hdr.highest_trackable_value";
    public static final String HDR_HIGHEST_TRACKABLE_VALUE_DEFAULT = "3600000000000L";
    public static final String HDR_NUM_SIGNIFICANT_VALUE_DIGITS = "metrics.hdr.num_significant_value_digits";
    public static final String HDR_NUM_SIGNIFICANT_VALUE_DIGITS_DEFAULT = "3";
    public static final String METRICS_INTERVAL_UNIT = "metrics.interval.unit";

    public Configuration(boolean bln) {
        super(bln);
        loadDefaultValues();
    }

    public Configuration() {
        loadDefaultValues();
    }

    public final void loadDefaultValues() {
        set(METRICS_ENABLED, METRICS_ENABLED_DEFAULT);
        set(METRICS_INTERVAL, METRICS_INTERVAL_DEFAULT);
        set(METRICS_MEMORY_ENABLED, METRICS_MEMORY_ENABLED_DEFAULT);
        set(METRICS_PROCESS_TIME_SAMPLE_RATE, METRICS_PROCESS_TIME_SAMPLE_RATE_DEFAULT);
        set(METRICS_STATSD_HOST, METRICS_STATSD_HOST_DEFAULT);
        set(METRICS_STATSD_PORT, METRICS_STATSD_PORT_DEFAULT);
        set(METRICS_ENABLED_METRICS, METRICS_ENABLED_METRICS_DEFAULT);

        set(HDR_HIGHEST_TRACKABLE_VALUE, HDR_HIGHEST_TRACKABLE_VALUE_DEFAULT);
        set(HDR_NUM_SIGNIFICANT_VALUE_DIGITS, HDR_NUM_SIGNIFICANT_VALUE_DIGITS_DEFAULT);
    }

    @Override
    public String toString() {
        Tuple2<String, String>[] tuples = getAll();
        String[] items = new String[tuples.length];

        for (int i = 0; i < tuples.length; i++) {
            items[i] = tuples[i]._1 + "=" + tuples[i]._2;
        }

        return StringUtils.join(items, ",");
    }

    public static Configuration fromMap(Map map) {
        Configuration config = new Configuration();

        for (Object k : map.keySet()) {
            String key = (String) k;
            Object value = map.get(key);

            if (value instanceof String) {
                String str = (String) value;
                config.set(key, str);
            } else {
                config.set(key, value.toString());
            }
        }

        return config;
    }

    public static Configuration fromProperties(Properties properties) {
        Configuration config = new Configuration();

        for (String key : properties.stringPropertyNames()) {
            config.set(key, properties.getProperty(key));
        }

        return config;
    }

    public static Configuration fromStr(String str) {
        Map<String, String> map = strToMap(str);
        Configuration config = new Configuration();

        for (String key : map.keySet()) {
            config.set(key, map.get(key));
        }

        return config;
    }

    public static Map<String, String> strToMap(String str) {
        Map<String, String> map = new HashMap<>();
        /*String[] arguments = str.split(",");
        
        for (String arg : arguments) {
            String[] kv = arg.split("=");
            map.put(kv[0].trim(), kv[1].trim());
        }*/
        Pattern p = Pattern.compile("([^=]+)=([^=]+)(?:,|$)");
        Matcher m = p.matcher(str);

        while (m.find()) {
            map.put(m.group(1).trim(), m.group(2).trim());
        }

        return map;
    }
}
