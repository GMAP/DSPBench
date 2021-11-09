package com.streamer.examples.adsanalytics;

import com.streamer.base.task.BasicTask;
import com.streamer.core.Schema;
import com.streamer.core.Stream;
import static com.streamer.examples.adsanalytics.AdsAnalyticsConstants.*;
import com.streamer.examples.adsanalytics.AdsAnalyticsConstants.Config;
import com.streamer.examples.adsanalytics.AdsAnalyticsConstants.Field;
import com.streamer.examples.adsanalytics.AdsAnalyticsConstants.Streams;
import com.streamer.examples.trendingtopics.TrendingTopicsConstants;
import com.streamer.utils.Configuration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author mayconbordin
 */
public class AdsAnalyticsTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(AdsAnalyticsTask.class);
    
    private int ctrThreads;
    private int ctrFrequency;

    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        ctrThreads   = config.getInt(Config.CTR_THREADS, 1);
        ctrFrequency = config.getInt(Config.CTR_EMIT_FREQUENCY, 60);
    }
    
    @Override
    public void initialize() {
        Schema clickSchema = new Schema().keys(Field.QUERY_ID, Field.AD_ID).fields(Field.EVENT);
        Schema ctrSchema   = new Schema(Field.QUERY_ID, Field.AD_ID, Field.CTR,
                                        Field.IMPRESSIONS, Field.CLICKS, Field.WINDOW_LENGTH);
        
        Stream clicks      = builder.createStream(Streams.CLICKS, clickSchema);
        Stream impressions = builder.createStream(Streams.IMPRESSIONS, clickSchema);
        Stream ctrs        = builder.createStream(Streams.CTRS, ctrSchema);
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, clicks);
        builder.publish(Component.SOURCE, impressions);
        builder.setTupleRate(Component.SOURCE, sourceRate);
        
        builder.setOperator(Component.CTR, new RollingCtrBolt(), ctrThreads);
        builder.setTimer(Component.CTR, ctrFrequency, TimeUnit.SECONDS);
        builder.groupByKey(Component.CTR, clicks);
        builder.groupByKey(Component.CTR, impressions);
        builder.publish(Component.CTR, ctrs);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.shuffle(Component.SINK, ctrs);
    }
    
    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
    
}
