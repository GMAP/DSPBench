package flink.parsers;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Configurations;
import flink.util.Metrics;

public class StockQuotesParser extends RichMapFunction<String, Tuple5<String, Double, Integer, DateTime, Integer>>{
    
    private static final Logger LOG = LoggerFactory.getLogger(StockQuotesParser.class);
    Configuration config;
    String sourceName;

    Metrics metrics = new Metrics();

    public StockQuotesParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    @Override
    public Tuple5<String, Double, Integer, DateTime, Integer> map(String value) throws Exception {
        DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
        //super.initialize(config);
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        String[] record = value.split(",");
        //super.incReceived();
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        if (record.length < 8)
            return null;

        // exclude header
        if (record[1].toLowerCase().equals("date")) {
            return null;
        }

        String stock      = record[0];
        DateTime date     = dtFormatter.parseDateTime(record[1]).withTimeAtStartOfDay();
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

        //super.incEmitted();
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }
        return new Tuple5<String,Double,Integer,DateTime,Integer>(stock, close, volume, date, interval);
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
    
}
