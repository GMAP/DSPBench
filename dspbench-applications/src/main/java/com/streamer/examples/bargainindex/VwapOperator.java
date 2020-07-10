package com.streamer.examples.bargainindex;

import com.streamer.base.operator.BaseOperator;
import com.streamer.core.Operator;
import com.streamer.core.Tuple;
import com.streamer.core.Values;
import com.streamer.examples.bargainindex.BargainIndexConstants.Config;
import com.streamer.examples.bargainindex.BargainIndexConstants.Field;
import com.streamer.examples.bargainindex.BargainIndexConstants.Periodicity;
import com.streamer.utils.Configuration;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class VwapOperator extends BaseOperator {
    private static final DateTimeComparator dateOnlyComparator = DateTimeComparator.getDateOnlyInstance();
    
    private Map<String, Vwap> stocks;
    private String period;

    @Override
    public void initialize() {        
        stocks = new HashMap<String, Vwap>();
        period = config.getString(Config.VWAP_PERIOD, Periodicity.DAILY);
    }

    @Override
    public void process(Tuple input) {
        String stock  = input.getString(Field.STOCK);
        double price  = input.getDouble(Field.PRICE);
        int volume    = input.getInt(Field.VOLUME);
        DateTime date = new DateTime((Date) input.getValue(Field.DATE));
        int inteval   = input.getInt(Field.INTERVAL);

        Vwap vwap = stocks.get(stock);

        if (withinPeriod(vwap, date)) {
            vwap.update(volume, price, date.plusSeconds(inteval));
            emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate().toDate(), vwap.getEndDate().toDate()));
        } else {
            if (vwap != null) {
                emit(new Values(stock, vwap.getVwap(), vwap.getStartDate().toDate(), vwap.getEndDate().toDate()));
            }
            
            vwap = new Vwap(volume, price, date, date.plusSeconds(inteval));
            stocks.put(stock, vwap);

            emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        }
    }
    
    private boolean withinPeriod(Vwap vwap, DateTime quoteDate) {
        if (vwap == null) return false;
        
        DateTime vwapDate  = vwap.getStartDate();
        
        if (period.equals(Periodicity.MINUTELY)) {
            return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0) && 
                    vwapDate.getMinuteOfDay() == quoteDate.getMinuteOfDay());
        } else if (period.equals(Periodicity.HOURLY)) {
            return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0) && 
                    vwapDate.getHourOfDay() == quoteDate.getHourOfDay());
        } else if (period.equals(Periodicity.DAILY)) {
            return (dateOnlyComparator.compare(vwapDate, quoteDate) == 0);
        } else if (period.equals(Periodicity.WEEKLY)) {
            return (vwapDate.getYear() == quoteDate.getYear() &&
                    vwapDate.getWeekOfWeekyear() == quoteDate.getWeekOfWeekyear());
        } else if (period.equals(Periodicity.MONTHLY)) {
            return (vwapDate.getYear() == quoteDate.getYear() &&
                    vwapDate.getMonthOfYear() == quoteDate.getMonthOfYear());
        }
        
        return false;
    }
    
    public static final class Vwap {
        private long totalShares = 0;
        private double tradedValue = 0;
        private double vwap = 0;
        private DateTime startDate;
        private DateTime endDate;

        public Vwap(long shares, double price, DateTime start, DateTime end) {
            this.startDate = start;
            this.endDate = end;
            
            update(shares, price, null);
        }
        
        public void update(long shares, double price, DateTime date) {
            totalShares += shares;
            tradedValue += (shares*price);
            vwap = tradedValue/totalShares;
            
            if (date != null) {
                endDate = date;
            }
        }

        public double getVwap() {
            return vwap;
        }

        public DateTime getStartDate() {
            return startDate;
        }

        public DateTime getEndDate() {
            return endDate;
        }
    }
    
}
