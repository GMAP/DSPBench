package org.dspbench.applications.adsanalytics;

import org.dspbench.base.task.BasicTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import org.dspbench.utils.Configuration;
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
        
        ctrThreads   = config.getInt(AdsAnalyticsConstants.Config.CTR_THREADS, 1);
        ctrFrequency = config.getInt(AdsAnalyticsConstants.Config.CTR_EMIT_FREQUENCY, 60);
    }
    
    @Override
    public void initialize() {
        Schema clickSchema = new Schema().keys(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID).fields(AdsAnalyticsConstants.Field.EVENT);
        Schema ctrSchema   = new Schema(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID, AdsAnalyticsConstants.Field.CTR,
                                        AdsAnalyticsConstants.Field.IMPRESSIONS, AdsAnalyticsConstants.Field.CLICKS, AdsAnalyticsConstants.Field.WINDOW_LENGTH);
        
        Stream clicks      = builder.createStream(AdsAnalyticsConstants.Streams.CLICKS, clickSchema);
        Stream impressions = builder.createStream(AdsAnalyticsConstants.Streams.IMPRESSIONS, clickSchema);
        Stream ctrs        = builder.createStream(AdsAnalyticsConstants.Streams.CTRS, ctrSchema);
        
        builder.setSource(AdsAnalyticsConstants.Component.SOURCE, source, sourceThreads);
        builder.publish(AdsAnalyticsConstants.Component.SOURCE, clicks);
        builder.publish(AdsAnalyticsConstants.Component.SOURCE, impressions);
        builder.setTupleRate(AdsAnalyticsConstants.Component.SOURCE, sourceRate);
        
        builder.setOperator(AdsAnalyticsConstants.Component.CTR, new RollingCtrBolt(), ctrThreads);
        builder.setTimer(AdsAnalyticsConstants.Component.CTR, ctrFrequency, TimeUnit.SECONDS);
        builder.groupByKey(AdsAnalyticsConstants.Component.CTR, clicks);
        builder.groupByKey(AdsAnalyticsConstants.Component.CTR, impressions);
        builder.publish(AdsAnalyticsConstants.Component.CTR, ctrs);
        
        builder.setOperator(AdsAnalyticsConstants.Component.SINK, sink, sinkThreads);
        builder.shuffle(AdsAnalyticsConstants.Component.SINK, ctrs);
    }
    
    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return AdsAnalyticsConstants.PREFIX;
    }
    
}
