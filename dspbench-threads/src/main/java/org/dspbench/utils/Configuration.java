package org.dspbench.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class Configuration extends HashMap<String, Object> {
    private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);
    
    public static final String METRICS_ENABLED  = "metrics.enabled";
    public static final Object METRICS_ENABLED_DEFAULT = false;
    
    public static final String METRICS_INTERVAL = "metrics.interval";
    public static final Object METRICS_INTERVAL_DEFAULT = 5;
    
    public static final String METRICS_PROCESS_TIME_SAMPLE_RATE = "metrics.process_time.sample_rate";
    public static final Object METRICS_PROCESS_TIME_SAMPLE_RATE_DEFAULT = 0.05;
    
    public static final String METRICS_MEMORY_ENABLED  = "metrics.memory.enabled";
    public static final Object METRICS_MEMORY_ENABLED_DEFAULT = false;
    
    public static final String METRICS_REPORTER = "metrics.reporter";
    public static final String METRICS_OUTPUT   = "metrics.output";
    
    public static final String METRICS_STATSD_HOST   = "metrics.statsd.host";
    public static final Object METRICS_STATSD_HOST_DEFAULT   = "localhost";
    public static final String METRICS_STATSD_PORT   = "metrics.statsd.port";
    public static final Object METRICS_STATSD_PORT_DEFAULT   = 8125;
    
    public static final String METRICS_ENABLED_METRICS = "metrics.enabled.metrics";
    public static final Object METRICS_ENABLED_METRICS_DEFAULT = "throughput,tuple-counter,tuple-latency-hdr";
    
    public static final String HDR_HIGHEST_TRACKABLE_VALUE = "metrics.hdr.highest_trackable_value";
    public static final Object HDR_HIGHEST_TRACKABLE_VALUE_DEFAULT = 3600000000000L;
    public static final String HDR_NUM_SIGNIFICANT_VALUE_DIGITS = "metrics.hdr.num_significant_value_digits";
    public static final Object HDR_NUM_SIGNIFICANT_VALUE_DIGITS_DEFAULT = 3;

    public Configuration() {
        loadDefaultValues();
    }

    public Configuration(Map<? extends String, ? extends Object> map) {
        loadDefaultValues();
        putAll(map);
    }
    
    public void loadDefaultValues() {
        put(METRICS_ENABLED, METRICS_ENABLED_DEFAULT);
        put(METRICS_INTERVAL, METRICS_INTERVAL_DEFAULT);
        put(METRICS_MEMORY_ENABLED, METRICS_MEMORY_ENABLED_DEFAULT);
        put(METRICS_PROCESS_TIME_SAMPLE_RATE, METRICS_PROCESS_TIME_SAMPLE_RATE_DEFAULT);
        put(METRICS_STATSD_HOST, METRICS_STATSD_HOST_DEFAULT);
        put(METRICS_STATSD_PORT, METRICS_STATSD_PORT_DEFAULT);
        put(METRICS_ENABLED_METRICS, METRICS_ENABLED_METRICS_DEFAULT);
        
        put(HDR_HIGHEST_TRACKABLE_VALUE, HDR_HIGHEST_TRACKABLE_VALUE_DEFAULT);
        put(HDR_NUM_SIGNIFICANT_VALUE_DIGITS, HDR_NUM_SIGNIFICANT_VALUE_DIGITS_DEFAULT);
    }
    
    public String getString(String key) {
        String val = null;
        Object obj = get(key);
        
        if (null != obj) {
            if (obj instanceof String) {
                val = (String)obj;
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public String getString(String key, String def) {
        String val = null;
        try {
            val = getString(key);
        } catch (IllegalArgumentException ex) {
            val = def;
        }
        return val;
    }

    public int getInt(String key) {
        int val = 0;
        Object obj = get(key);
        
        if (null != obj) {
            if (obj instanceof Integer) {
                val = (Integer)obj;
            } else if (obj instanceof Number) {
                val = ((Number)obj).intValue();
            } else if (obj instanceof String) {
                try {
                    val = Integer.parseInt((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        
        return val;
    }

    public long getLong(String key) {
        long val = 0;
        Object obj = get(key);
        
        if (null != obj) {
            if (obj instanceof Long) {
                val = (Long)obj;
            } else if (obj instanceof Number) {
                val = ((Number)obj).longValue();
            } else if (obj instanceof String) {
                try {
                    val = Long.parseLong((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found  in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public int getInt(String key, int def) {
        int val = 0;
        try {
            val = getInt(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public int[] getIntArray(String key, int[] def) {
        return getIntArray(key, ",", def);
    }

    public int[] getIntArray(String key, String separator, int[] def) {
        int[] values = null;

        try {
            values = getIntArray(key, separator);
        } catch (IllegalArgumentException ex) {
            values = def;
        }

        return values;
    }

    public int[] getIntArray(String key, String separator) {
        String value   = getString(key);
        String[] items = value.split(separator);
        int[] values   = new int[items.length];

        for (int i=0; i<items.length; i++) {
            try {
                values[i] = Integer.parseInt(items[i]);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Value for configuration key "
                        + key + " cannot be parsed to an Integer array", ex);
            }
        }

        return values;
    }

    public long getLong(String key, long def) {
        long val = 0;
        try {
            val = getLong(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public double getDouble(String key) {
        double  val = 0;
        Object obj = get(key);
        
        if (null != obj) {
            if (obj instanceof Double) {
                val = (Double)obj;
            } else if (obj instanceof Number) {
                val = ((Number)obj).doubleValue();
            } else if (obj instanceof String) {
                try {
                    val = Double.parseDouble((String)obj);
                } catch (NumberFormatException ex) {
                    throw new IllegalArgumentException("String value not found in configuration for " + key);
                }
            } else {
                throw new IllegalArgumentException("String value not found in configuration for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public double getDouble(String key, double def) {
        double val = 0;
        try {
            val = getDouble(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public boolean getBoolean(String key) {
        boolean val = false;
        Object obj = get(key);
        if (null != obj) {
            if (obj instanceof Boolean) {
                val = (Boolean)obj;
            } else if (obj instanceof String) {
                val = Boolean.parseBoolean((String)obj);
            } else {
                throw new IllegalArgumentException("Boolean value not found  in configuration  for " + key);
            }
        } else {
            throw new IllegalArgumentException("Nothing found in configuration for " + key);
        }
        return val;
    }

    public boolean getBoolean(String key, boolean def) {
        boolean val = false;
        try {
            val = getBoolean(key);
        } catch (Exception ex) {
            val = def;
        }
        return val;
    }

    public boolean exists(String key) {
        return containsKey(key);
    }
    
    public Map<String, String> toStringMap() {
        Map<String, String> map = new HashMap<String, String>();
        
        for (Map.Entry<String, Object> e : entrySet()) {
            map.put(e.getKey(), String.valueOf(e.getValue()));
        }
        
        return map;
    }
    
    public static Configuration fromStr(String str) {
        Map<String, String> map = strToMap(str);
        Configuration cfg = new Configuration();
        
        for (String key : map.keySet()) {
            cfg.put(key, parseString(map.get(key)));
        }
        
        return cfg;
    }
    
    public static Configuration fromProperties(String fileName) {
        return fromProperties(toProperties(fileName));
    }

    public static Configuration fromProperties(Properties properties) {
        Configuration cfg = new Configuration();
        
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            cfg.put(key, value);
        }
        
        return cfg;
    }
    
    public static Configuration fromMap(Map<String, Object> map) {
        Configuration cfg = new Configuration();
        
        if (map == null) return cfg;
        
        for (String key : map.keySet()) {
            Object value = map.get(key);
            
            if (value instanceof String) {
                cfg.put(key, parseString((String) value));
            } else {
                cfg.put(key, value);
            }
        }
        
        return cfg;
    }
    
    public static Properties toProperties(String filename) {
        Properties p = new Properties();
        InputStream is = null;
        
        try {
            is = new FileInputStream(filename);
            p.load(is);
            return p;
        } catch (FileNotFoundException ex) {
            LOG.error("File " + filename + " was not found", ex);
        } catch (IOException ex) {
            LOG.error("Error reading " + filename + " file", ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException ex) {
                    LOG.error("Error closing " + filename + " file", ex);
                }
            }
        }
        
        return null;
    }
    
    public static Map<String, String> strToMap(String str) {
        Map<String, String> map = new HashMap<String, String>();
        
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
    
    public static Map<String, Object> propsToMap(String fileName) {
        Properties properties = toProperties(fileName);
        Map<String, Object> map = new HashMap<String, Object>();
        
        for (String key : properties.stringPropertyNames()) {
            String value = properties.getProperty(key);
            map.put(key, value);
        }
        
        return map;
    }
    
    private static Object parseString(String value) {
        if (DataTypeUtils.isInteger(value)) {
            return Integer.parseInt(value);
        } else if (NumberUtils.isNumber(value)) {
            return Double.parseDouble(value);
        } else if (value.equals("true") || value.equals("false")) {
            return Boolean.parseBoolean(value);
        }
        
        return value;
    }
}
