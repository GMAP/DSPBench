package org.dspbench.applications.adsanalytics;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdsAnalyticsTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AdsAnalyticsTopology.class);
    
    private int ctrThreads;
    private int ctrFrequency;

    public AdsAnalyticsTopology(String topologyName, Config config) {
        super(topologyName, config);
    }
    
    @Override
    public void initialize() {
        super.initialize();
        
        ctrThreads   = config.getInt(AdsAnalyticsConstants.Conf.CTR_THREADS, 1);
        ctrFrequency = config.getInt(AdsAnalyticsConstants.Conf.CTR_EMIT_FREQUENCY, 60);
    }

    @Override
    public StormTopology buildTopology() {
        Fields spoutFields = new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID, AdsAnalyticsConstants.Field.EVENT);
        spout.setFields(AdsAnalyticsConstants.Stream.CLICKS, spoutFields);
        spout.setFields(AdsAnalyticsConstants.Stream.IMPRESSIONS, spoutFields);
        
        builder.setSpout(AdsAnalyticsConstants.Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(AdsAnalyticsConstants.Component.CTR, new RollingCtrBolt(ctrFrequency), ctrThreads)
               .fieldsGrouping(AdsAnalyticsConstants.Component.SPOUT, AdsAnalyticsConstants.Stream.CLICKS, new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID))
               .fieldsGrouping(AdsAnalyticsConstants.Component.SPOUT, AdsAnalyticsConstants.Stream.IMPRESSIONS, new Fields(AdsAnalyticsConstants.Field.QUERY_ID, AdsAnalyticsConstants.Field.AD_ID));

        builder.setBolt(AdsAnalyticsConstants.Component.SINK, sink, sinkThreads)
               .shuffleGrouping(AdsAnalyticsConstants.Component.CTR);

        return builder.createTopology();
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
