package flink.parsers;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.adanalytics.AdEvent;
import flink.util.Configurations;
import flink.util.Metrics;

public class AdEventParser extends RichFlatMapFunction<String, Tuple3<Long, Long, AdEvent>> {

    private static final Logger LOG = LoggerFactory.getLogger(AdEventParser.class);

    Configuration config;
    String sourceName;

    Metrics metrics = new Metrics();

    public AdEventParser(Configuration config, String sourceName){
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        this.config = config;
        this.sourceName = sourceName;
    }

    @Override
    public void flatMap(String value, Collector<Tuple3<Long, Long, AdEvent>> out) throws Exception {
        //super.initialize(config);
        metrics.initialize(config, this.getClass().getSimpleName()+"-"+sourceName);
        //super.incReceived();

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }

        String[] record = value.split("\t");
        
        if (record.length != 12)
            out.collect(null);

        int clicks         = Integer.parseInt(record[0]);
        int views          = Integer.parseInt(record[1]);
        String displayUrl  = record[2];
        long adId          = Long.parseLong(record[3]);
        long advertiserId  = Long.parseLong(record[4]);
        int depth          = Integer.parseInt(record[5]);
        int position       = Integer.parseInt(record[6]);
        long queryId       = Long.parseLong(record[7]);
        long keywordId     = Long.parseLong(record[8]);
        long titleId       = Long.parseLong(record[9]);
        long descriptionId = Long.parseLong(record[10]);
        long userId        = Long.parseLong(record[11]);

        for (int i=0; i<views+clicks; i++) {
            AdEvent event = new AdEvent(displayUrl, queryId, adId, userId, advertiserId,
                keywordId, titleId, descriptionId, depth, position);
            
            event.setType((i < views) ? AdEvent.Type.Impression : AdEvent.Type.Click);

            //super.incEmitted();
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.emittedThroughput();
            }

            out.collect(new Tuple3<Long, Long, AdEvent>(queryId, adId, event));
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
