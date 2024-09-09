package flink.util;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Configurations extends Configuration implements Serializable {
    public static final String GEOIP_INSTANCE = "geoip.instance";
    public static final String GEOIP2_DB = "geoip2.db";

    public static final String METRICS_ENABLED        = "metrics.enabled";
    public static final String METRICS_ONLY_SINK      = "metrics.onlySink";
    public static final String METRICS_INTERVAL_UNIT  = "metrics.interval.unit";
    public static final String METRICS_OUTPUT         = "metrics.output";


    public static Configuration fromMap(Map map) {
        Configuration config = new Configuration();
        
        for (Object k : map.keySet()) {
            ConfigOption<String> key = (ConfigOption<String>) k;
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
            ConfigOption<String> Key = ConfigOptions
                    .key(key)
                    .stringType()
                    .noDefaultValue();
            config.set(Key, properties.getProperty(key));
        }
        
        return config;
    }
    
    public static Configuration fromStr(String str) {
        Map<String, String> map = strToMap(str);
        Configuration config = new Configuration();
        
        for (String key : map.keySet()) {
            ConfigOption<String> Key = ConfigOptions
                    .key(key)
                    .stringType()
                    .noDefaultValue();
            config.set(Key, map.get(key));
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

        while(m.find()){
            map.put(m.group(1).trim(), m.group(2).trim());
        }
        
        return map;
    }
}
