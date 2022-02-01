package org.dspbench.applications.bargainindex;

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
public class BargainIndexTopology extends BasicTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BargainIndexBolt.class);
    
    private int vwapThreads;
    private int bargainIndexThreads;
    
    public BargainIndexTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        super.initialize();
        
        vwapThreads         = config.getInt(BargainIndexConstants.Conf.VWAP_THREADS, 1);
        bargainIndexThreads = config.getInt(BargainIndexConstants.Conf.BARGAIN_INDEX_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        spout.setFields(BargainIndexConstants.Stream.TRADES, new Fields(BargainIndexConstants.Field.STOCK, BargainIndexConstants.Field.PRICE, BargainIndexConstants.Field.VOLUME, BargainIndexConstants.Field.DATE, BargainIndexConstants.Field.INTERVAL));
        spout.setFields(BargainIndexConstants.Stream.QUOTES, new Fields(BargainIndexConstants.Field.STOCK, BargainIndexConstants.Field.PRICE, BargainIndexConstants.Field.VOLUME, BargainIndexConstants.Field.DATE, BargainIndexConstants.Field.INTERVAL));
        
        builder.setSpout(BargainIndexConstants.Component.SPOUT, spout, spoutThreads);

        builder.setBolt(BargainIndexConstants.Component.VWAP , new VwapBolt(), vwapThreads)
               .fieldsGrouping(BargainIndexConstants.Component.SPOUT, BargainIndexConstants.Stream.TRADES, new Fields(BargainIndexConstants.Field.STOCK));
        
        builder.setBolt(BargainIndexConstants.Component.BARGAIN_INDEX, new BargainIndexBolt(), bargainIndexThreads)
               .fieldsGrouping(BargainIndexConstants.Component.VWAP, new Fields(BargainIndexConstants.Field.STOCK))
               .fieldsGrouping(BargainIndexConstants.Component.SPOUT, BargainIndexConstants.Stream.QUOTES, new Fields(BargainIndexConstants.Field.STOCK));
        
        builder.setBolt(BargainIndexConstants.Component.SINK, sink, sinkThreads)
               .fieldsGrouping(BargainIndexConstants.Component.BARGAIN_INDEX, new Fields(BargainIndexConstants.Field.STOCK));
        
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return BargainIndexConstants.PREFIX;
    }
    
}
