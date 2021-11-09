package com.streamer.examples.bargainindex;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import static com.streamer.examples.bargainindex.BargainIndexConstants.*;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class BargainIndexOperator extends BaseOperator {
    private Map<String, TradeSummary> trades;
    private double threshold;

    @Override
    public void initialize() {
        trades = new HashMap<String, TradeSummary>();
        threshold = config.getDouble(Config.BARGAIN_INDEX_THRESHOLD, 0.001);
    }

    @Override
    public void process(Tuple input) {
        String streamId = input.getStreamId();
        
        if (streamId.equals(Streams.QUOTES)) {
            String stock    = input.getString(Field.STOCK);
            double askPrice = input.getDouble(Field.PRICE);
            int askSize     = input.getInt(Field.VOLUME);
            Date date       = (Date) input.getValue(Field.DATE);
            
            TradeSummary summary = trades.get(stock);
            double bargainIndex = 0;
            
            if (summary != null) {
                if (summary.vwap > askPrice) {
                    bargainIndex = Math.exp(summary.vwap - askPrice) * askSize;
                    
                    if (bargainIndex > threshold)
                        emit(input, new Values(stock, askPrice, askSize, bargainIndex));
                }
            }
        } else {
            String stock = input.getString(Field.STOCK);
            double vwap  = input.getDouble(Field.VWAP);
            Date endDate = (Date) input.getValue(Field.DATE);

            if (trades.containsKey(stock)) {
                TradeSummary summary = trades.get(stock);
                summary.vwap = vwap;
                summary.date = endDate;
            } else {
                trades.put(stock, new TradeSummary(stock, vwap, endDate));
            }
        }
    }
    
    private static class TradeSummary implements Serializable {
        public String symbol;
        public double vwap;
        public Date date;

        public TradeSummary(String symbol, double vwap, Date date) {
            this.symbol = symbol;
            this.vwap = vwap;
            this.date = date;
        }
    }
}
