package com.streamer.examples.bargainindex;

import com.google.common.collect.ImmutableList;
import com.streamer.base.source.parser.Parser;
import com.streamer.core.Values;
import com.streamer.examples.adsanalytics.AdEvent;
import com.streamer.examples.adsanalytics.AdsAnalyticsConstants;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.List;

public class StockQuotesParser extends Parser {
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    private static final int DATE      = 0;
    private static final int OPEN      = 1;
    private static final int HIGH      = 2;
    private static final int LOW       = 3;
    private static final int CLOSE     = 4;
    private static final int ADJ_CLOSE = 5;
    private static final int VOLUME    = 6;

    @Override
    public List<Values> parse(String filename, String input) {
        String[] record = input.split(",");
        String[] name   = filename.split("\\.");
        
        if (record.length != 7)
            return null;

        // exclude header
        if (record[DATE].toLowerCase().equals("date")) {
            return null;
        }

        String stock      = name[0];
        Date date         = dtFormatter.parseLocalDate(record[DATE]).toDate();
        double open       = Double.parseDouble(record[OPEN]);
        double high       = Double.parseDouble(record[HIGH]);
        double low        = Double.parseDouble(record[LOW]);
        double close      = Double.parseDouble(record[CLOSE]);
        Integer volume = null;

        try {
            volume = Integer.parseInt(record[VOLUME]);
        } catch (NumberFormatException e) {
            Double number = Double.parseDouble(record[VOLUME]);
            volume = number.intValue();
        }

        int interval     = 24 * 60 * 60;

        Values tradeValues = new Values(stock, close, volume, date, interval);
        tradeValues.setStreamId(BargainIndexConstants.Streams.TRADES);

        Values quoteValues = new Values(stock, close, volume, date, interval);
        quoteValues.setStreamId(BargainIndexConstants.Streams.QUOTES);

        return ImmutableList.of(tradeValues, quoteValues);
    }

    @Override
    public List<Values> parse(String str) {
        return null;
    }
}