package flink.application.bargainindex;

import java.io.*;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import flink.constants.BargainIndexConstants;
import flink.constants.BaseConstants;
import flink.util.Configurations;
import flink.util.Metrics;
import flink.util.MetricsFactory;

public class BargainingIndex extends RichCoFlatMapFunction<Tuple4<String, Double, DateTime, DateTime>, Tuple5<String, Double, Integer, DateTime, Integer>, Tuple4<String, Double, Integer, Double>> {
    private Map<String, TradeSummary> trades;
    private double threshold;
    Configuration config;

    Metrics metrics = new Metrics();

    public BargainingIndex(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;

        threshold = config.getDouble(BargainIndexConstants.Conf.BARGAIN_INDEX_THRESHOLD, 0.001);
        trades = new HashMap<>();
    }

    private static class TradeSummary {
        public String symbol;
        public double vwap;
        public DateTime date;

        public TradeSummary(String symbol, double vwap, DateTime date) {
            this.symbol = symbol;
            this.vwap = vwap;
            this.date = date;
        }
    }

    @Override
    public void flatMap1(Tuple4<String, Double, DateTime, DateTime> value,
            Collector<Tuple4<String, Double, Integer, Double>> out) throws Exception {
        // VWAP
        metrics.initialize(config, this.getClass().getSimpleName());
        //metrics.incReceived("BargainingIndex");
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String stock = value.getField(0);
        double vwap  = (Double) value.getField(1);
        DateTime endDate = (DateTime) value.getField(3);

        if (trades.containsKey(stock)) {
            TradeSummary summary = trades.get(stock);
            summary.vwap = vwap;
            summary.date = endDate;
        } else {
            trades.put(stock, new TradeSummary(stock, vwap, endDate));
        }
    }

    @Override
    public void flatMap2(Tuple5<String, Double, Integer, DateTime, Integer> value,
            Collector<Tuple4<String, Double, Integer, Double>> out) throws Exception {
        // QUOTES
        metrics.initialize(config, this.getClass().getSimpleName());
        //metrics.incReceived("BargainingIndex");
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        String stock    = value.getField(0);
        double askPrice = value.getField(1);
        int askSize     = value.getField(2);
        DateTime date   = (DateTime) value.getField(3);
        
        TradeSummary summary = trades.get(stock);
        double bargainIndex = 0;
        
        if (summary != null) {
            if (summary.vwap > askPrice) {
                bargainIndex = Math.exp(summary.vwap - askPrice) * askSize;
                
                if (bargainIndex > threshold)
                    //metrics.incEmitted("BargainingIndex");
                    if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                        metrics.emittedThroughput();
                    }
                    //collector.emit(new Values(stock, askPrice, askSize, bargainIndex));
                    out.collect(new Tuple4<String,Double,Integer,Double>(stock, askPrice, askSize, bargainIndex));
            }
        }
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }
}