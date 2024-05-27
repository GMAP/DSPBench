package flink.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.adanalytics.AdEvent;

public class AdEventParser extends Parser implements FlatMapFunction<String, Tuple3<Long, Long, AdEvent>> {

    private static final Logger LOG = LoggerFactory.getLogger(AdEventParser.class);

    Configuration config;

    public AdEventParser(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(String value, Collector<Tuple3<Long, Long, AdEvent>> out) throws Exception {
        super.initialize(config);
        super.incReceived();
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

            super.incEmitted();
            out.collect(new Tuple3<Long, Long, AdEvent>(queryId, adId, event));
        }
    }


    @Override
    public Tuple1<?> parse(String input) {
        return null;
    }
    
}
