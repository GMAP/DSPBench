package flink.application.adanalytics;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.AbstractApplication;
import flink.constants.AdAnalyticsConstants;
import flink.parsers.AdEventParser;

public class AdAnalytics extends AbstractApplication{

    private static final Logger LOG = LoggerFactory.getLogger(AdAnalytics.class);

    private int clickParserThreads;
    private int impressionsParserThreads;
    private int CTRThreads;
    private int CTRLength;
    private int CTRFrequency;

    public AdAnalytics(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        clickParserThreads = config.getInteger(AdAnalyticsConstants.Conf.CLICKS_PARSER_THREADS, 1);
        impressionsParserThreads = config.getInteger(AdAnalyticsConstants.Conf.IMPRESSIONS_PARSER_THREADS, 1);
        CTRThreads = config.getInteger(AdAnalyticsConstants.Conf.CTR_THREADS, 1);
        CTRLength = config.getInteger(AdAnalyticsConstants.Conf.CTR_WINDOW_LENGTH, 1);
        CTRFrequency = config.getInteger(AdAnalyticsConstants.Conf.CTR_EMIT_FREQUENCY, 60);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        DataStream<String> clicks = createSource("click");
        DataStream<String> impressions = createSource("impressions");

        // Parser
        DataStream<Tuple3<Long, Long, AdEvent>> clicksParser = clicks.flatMap(new AdEventParser(config, "clicks")).setParallelism(clickParserThreads);
        DataStream<Tuple3<Long, Long, AdEvent>> impressionsParser = impressions.flatMap(new AdEventParser(config, "impressions")).setParallelism(impressionsParserThreads);

        // Process
        DataStream<Tuple6<String, String, Double, Long, Long, Integer>> rollingCTR = clicksParser.union(impressionsParser).keyBy(
            new KeySelector<Tuple3<Long,Long,AdEvent>,Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> getKey(Tuple3<Long,Long,AdEvent> value){
                    return Tuple2.of(value.f0, value.f1);
                }
            }
        ).window(TumblingProcessingTimeWindows.of(Time.seconds(CTRLength)))
        .apply(new RollingCTR(config, CTRFrequency))
        .setParallelism(CTRThreads);

        createSinkAA(rollingCTR);

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return AdAnalyticsConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
    
}
