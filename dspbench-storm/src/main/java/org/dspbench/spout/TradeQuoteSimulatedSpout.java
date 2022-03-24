package org.dspbench.spout;

import org.apache.storm.tuple.Values;
import org.dspbench.applications.bargainindex.BargainIndexConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dspbench.applications.bargainindex.Quote;
import org.dspbench.util.math.RandomUtil;

/**
 * Fetches 
 * @author mayconbordin <mayconbordin@gmail.com>
 */
public class TradeQuoteSimulatedSpout extends StockPriceSpout {
    private static Logger LOG = LoggerFactory.getLogger(TradeQuoteSimulatedSpout.class);

    @Override
    public void nextTuple() {
        Quote quote = queue.poll();
        
        if (quote != null) {
            collector.emit(BargainIndexConstants.Stream.TRADES, new Values(quote.getSymbol(), quote.getAverage(), quote.getVolume(), quote.getOpenDate(), quote.getInterval()));
            
            // here we simulate an askPrice that range between 90% and 110% of the
            // average current price for the same symbol
            double askPrice = RandomUtil.randDouble(quote.getAverage()*0.9, quote.getAverage()*1.1);
            
            // the size offered ranges from 10% to 80% of the volume
            int askSize = RandomUtil.randInt((int)(quote.getVolume()*0.1), (int)(quote.getVolume()*0.8));
            
            collector.emit(BargainIndexConstants.Stream.QUOTES, new Values(quote.getSymbol(), askPrice, askSize, quote.getOpenDate().plusMinutes(2), quote.getInterval()));
        }
    }
    
}
