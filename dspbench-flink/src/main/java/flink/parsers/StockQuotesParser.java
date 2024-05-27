package flink.parsers;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class StockQuotesParser extends Parser implements MapFunction<String, Tuple5<String, Double, Integer, Date, Integer>>{
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
    private static final Logger LOG = LoggerFactory.getLogger(StockQuotesParser.class);
    Configuration config;

    public StockQuotesParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public Tuple5<String, Double, Integer, Date, Integer> map(String value) throws Exception {
        super.initialize(config);
        String[] record = value.split(",");
        super.incReceived();

        if (record.length != 7)
            return null;

        // exclude header
        if (record[1].toLowerCase().equals("date")) {
            return null;
        }

        String stock      = record[0];
        Date date         = dtFormatter.parseLocalDate(record[1]).toDate();
        double open       = Double.parseDouble(record[2]);
        double high       = Double.parseDouble(record[3]);
        double low        = Double.parseDouble(record[4]);
        double close      = Double.parseDouble(record[5]);
        Integer volume = null;

        try {
            volume = Integer.parseInt(record[7]);
        } catch (NumberFormatException e) {
            Double number = Double.parseDouble(record[7]);
            volume = number.intValue();
        }

        int interval = 24 * 60 * 60;

        super.incEmitted();
        return new Tuple5<String,Double,Integer,Date,Integer>(stock, close, volume, date, interval);
    }

    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
    
}
