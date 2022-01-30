package org.dspbench.applications.bargainindex;

import org.dspbench.base.task.BasicTask;
import org.dspbench.core.Schema;
import org.dspbench.core.Stream;
import static org.dspbench.applications.bargainindex.BargainIndexConstants.*;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class BargainIndexTask extends BasicTask {
    private static final Logger LOG = LoggerFactory.getLogger(BargainIndexTask.class);
    
    private int vwapThreads;
    private int bargainIndexThreads;
    
    @Override
    public void setConfiguration(Configuration config) {
        super.setConfiguration(config);
        
        vwapThreads = config.getInt(Config.VWAP_THREADS, 1);
        bargainIndexThreads = config.getInt(Config.BARGAIN_INDEX_THREADS, 1);
    }

    public void initialize() {
        Schema tradeQuoteSchema = new Schema().keys(Field.STOCK).fields(Field.PRICE, Field.VOLUME, Field.DATE, Field.INTERVAL);
        Schema vwapSchema = new Schema().keys(Field.STOCK).fields(Field.VWAP, Field.START_DATE, Field.END_DATE);
        Schema bargainSchema = new Schema().keys(Field.STOCK).fields(Field.PRICE, Field.VOLUME, Field.BARGAIN_INDEX);
        
        Stream trades   = builder.createStream(Streams.TRADES, tradeQuoteSchema);
        Stream quotes   = builder.createStream(Streams.QUOTES, tradeQuoteSchema);
        Stream vwap     = builder.createStream(Streams.VWAP, vwapSchema);
        Stream bargains = builder.createStream(Streams.BARGAINS, bargainSchema);
        
        builder.setSource(Component.SOURCE, source, sourceThreads);
        builder.publish(Component.SOURCE, trades, quotes);
        
        builder.setOperator(Component.VWAP, new VwapOperator(), vwapThreads);
        builder.groupByKey(Component.VWAP, trades);
        builder.publish(Component.VWAP, vwap);
        
        builder.setOperator(Component.BARGAIN_INDEX, new BargainIndexOperator(), bargainIndexThreads);
        builder.groupByKey(Component.BARGAIN_INDEX, vwap);
        builder.groupByKey(Component.BARGAIN_INDEX, quotes);
        builder.publish(Component.BARGAIN_INDEX, bargains);
        
        builder.setOperator(Component.SINK, sink, sinkThreads);
        builder.groupByKey(Component.SINK, bargains);
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
