package org.dspbench.applications.bargainindex;

import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.dspbench.applications.reinforcementlearner.ReinforcementLearnerConstants;
import org.dspbench.sink.BaseSink;
import org.dspbench.spout.AbstractSpout;
import org.dspbench.topology.AbstractTopology;
import org.dspbench.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BargainIndexTopology extends AbstractTopology {
    private static final Logger LOG = LoggerFactory.getLogger(BargainIndexBolt.class);

    private AbstractSpout tradesSpout;
    private AbstractSpout quotesSpout;
    private BaseSink sink;

    private int tradesSpoutThreads;
    private int quotesSpoutThreads;
    private int sinkThreads;


    private int vwapThreads;
    private int bargainIndexThreads;
    
    public BargainIndexTopology(String topologyName, Config config) {
        super(topologyName, config);
    }

    @Override
    public void initialize() {
        tradesSpout  = loadSpout("trades");
        quotesSpout = loadSpout("quotes");
        sink  = loadSink();

        tradesSpoutThreads = config.getInt(getConfigKey(ReinforcementLearnerConstants.Conf.SPOUT_THREADS, "trades"), 1);
        quotesSpoutThreads = config.getInt(getConfigKey(ReinforcementLearnerConstants.Conf.SPOUT_THREADS, "quotes"), 1);
        sinkThreads = config.getInt(getConfigKey(ReinforcementLearnerConstants.Conf.SINK_THREADS), 1);
        
        vwapThreads         = config.getInt(BargainIndexConstants.Conf.VWAP_THREADS, 1);
        bargainIndexThreads = config.getInt(BargainIndexConstants.Conf.BARGAIN_INDEX_THREADS, 1);
    }

    @Override
    public StormTopology buildTopology() {
        tradesSpout.setFields(new Fields(BargainIndexConstants.Field.STOCK, BargainIndexConstants.Field.PRICE, BargainIndexConstants.Field.VOLUME, BargainIndexConstants.Field.DATE, BargainIndexConstants.Field.INTERVAL));
        quotesSpout.setFields(new Fields(BargainIndexConstants.Field.STOCK, BargainIndexConstants.Field.PRICE, BargainIndexConstants.Field.VOLUME, BargainIndexConstants.Field.DATE, BargainIndexConstants.Field.INTERVAL));
        
        builder.setSpout(BargainIndexConstants.Component.TRADES_SPOUT, tradesSpout, tradesSpoutThreads);
        builder.setSpout(BargainIndexConstants.Component.QUOTES_SPOUT, quotesSpout, quotesSpoutThreads);

        builder.setBolt(BargainIndexConstants.Component.VWAP , new VwapBolt(), vwapThreads)
               .fieldsGrouping(BargainIndexConstants.Component.TRADES_SPOUT, new Fields(BargainIndexConstants.Field.STOCK));

        builder.setBolt(BargainIndexConstants.Component.BARGAIN_INDEX, new BargainIndexBolt(), bargainIndexThreads)
               .fieldsGrouping(BargainIndexConstants.Component.VWAP, new Fields(BargainIndexConstants.Field.STOCK))
               .fieldsGrouping(BargainIndexConstants.Component.QUOTES_SPOUT, new Fields(BargainIndexConstants.Field.STOCK));
        
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
