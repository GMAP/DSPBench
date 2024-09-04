package flink.application.trendingtopics;

import java.util.Date;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.util.Configurations;
import flink.util.Metrics;

public class TopicExtractor extends RichFlatMapFunction<Tuple3<String, String, Date>, Tuple1<String>>{
    private static final Logger LOG = LoggerFactory.getLogger(TopicExtractor.class);

    Configuration config;
    Metrics metrics = new Metrics();

    public TopicExtractor(Configuration config){
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
    }

    @Override
    public void flatMap(Tuple3<String, String, Date>value, Collector<Tuple1<String>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        String text = (String) value.getField(1);
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.receiveThroughput();
        }
        
        if (text != null) {
            StringTokenizer st = new StringTokenizer(text);

            while (st.hasMoreElements()) {
                String term = (String) st.nextElement();
                if (StringUtils.startsWith(term, "#")) {
                    if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                        metrics.emittedThroughput();
                    }
                    //collector.emit(input, new Values(term));
                    out.collect(new Tuple1<String>(term));
                }
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
