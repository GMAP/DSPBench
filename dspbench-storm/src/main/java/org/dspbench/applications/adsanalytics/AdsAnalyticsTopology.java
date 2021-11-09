package org.dspbench.applications.adsanalytics;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.applications.adsanalytics.AdsAnalyticsConstants.*;

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
        
        ctrThreads   = config.getInt(Conf.CTR_THREADS, 1);
        ctrFrequency = config.getInt(Conf.CTR_EMIT_FREQUENCY, 60);
    }

    @Override
    public StormTopology buildTopology() {
        Fields spoutFields = new Fields(Field.QUERY_ID, Field.AD_ID, Field.EVENT);
        spout.setFields(Stream.CLICKS, spoutFields);
        spout.setFields(Stream.IMPRESSIONS, spoutFields);
        
        builder.setSpout(Component.SPOUT, spout, spoutThreads);
        
        builder.setBolt(Component.CTR, new RollingCtrBolt(ctrFrequency), ctrThreads)
               .fieldsGrouping(Component.SPOUT, Stream.CLICKS, new Fields(Field.QUERY_ID, Field.AD_ID))
               .fieldsGrouping(Component.SPOUT, Stream.IMPRESSIONS, new Fields(Field.QUERY_ID, Field.AD_ID));

        builder.setBolt(Component.SINK, sink, sinkThreads)
               .shuffleGrouping(Component.CTR);

        return builder.createTopology();
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
