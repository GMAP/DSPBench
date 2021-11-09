package com.streamer.examples.bargainindex;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

public class Quote implements Serializable {
    private Date openDate;
    private int interval;
    private BigDecimal open;
    private BigDecimal high;
    private BigDecimal low;
    private BigDecimal close;
    private int volume;

    public Quote(Date openDate, int interval, BigDecimal open, BigDecimal high, BigDecimal low, BigDecimal close, int volume) {
        this.openDate = openDate;
        this.interval = interval;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    @Override
    public String toString() {
        return "Date = " + openDate + ", OHLC = " + open + "/" + high + "/"
                + low + "/" + close + ", Volume = " + volume;
    }

    public Date getOpenDate() {
        return openDate;
    }

    public int getInterval() {
        return interval;
    }

    public BigDecimal getOpen() {
        return open;
    }

    public BigDecimal getHigh() {
        return high;
    }

    public BigDecimal getLow() {
        return low;
    }

    public BigDecimal getClose() {
        return close;
    }

    public int getVolume() {
        return volume;
    }
    
    public double getAverage() {
        return (open.doubleValue()+high.doubleValue()+low.doubleValue()+close.doubleValue())/4;
    }
}
