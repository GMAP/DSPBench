package com.streamer.core;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface Constants {
    public static final String DEFAULT_STREAM = "default";
    
    public static final double DEFAULT_RATE = 2000.0;
    
    public static final String METRICS_REPORTER_CONSOLE = "console";
    public static final String METRICS_REPORTER_SLF4J = "slf4j";
    public static final String METRICS_REPORTER_CSV = "csv";
    public static final String METRICS_REPORTER_STATSD = "statsd";
    
    public static final String METRIC_TUPLE_LATENCY = "tuple-latency";
    public static final String METRIC_THROUGHPUT = "throughput";
    public static final String METRIC_TUPLE_COUNTER = "tuple-counter";
    public static final String METRIC_TUPLE_LATENCY_HDR = "tuple-latency-hdr";
    public static final String METRIC_TUPLE_SIZE = "tuple-size";
    public static final String METRIC_PROCESS_TIME = "process-time";
}
