package flink.application.trendingtopics;

import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.TrendingTopicsConstants;
import flink.util.Metrics;

public class TopicExtractor extends Metrics implements FlatMapFunction<Tuple1<JSONObject>, Tuple1<String>>{
    private static final Logger LOG = LoggerFactory.getLogger(TopicExtractor.class);

    Configuration config;

    public TopicExtractor(Configuration config){
        super.initialize(config);
        this.config = config;
    }

    @Override
    public void flatMap(Tuple1<JSONObject> value, Collector<Tuple1<String>> out) throws Exception {
        super.initialize(config);
        Map tweet = (Map) value.getField(0);
        String text = (String) tweet.get("text");
        super.incReceived();
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    super.incEmitted();
                    //collector.emit(input, new Values(term));
                    out.collect(new Tuple1<String>(term));
                }
            }
        }
    }
    
}
