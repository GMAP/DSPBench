package flink.application.trendingtopics;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.tools.Rankable;
import flink.tools.RankableObjectWithFields;
import flink.tools.Rankings;
import flink.util.Configurations;
import flink.util.Metrics;

public class IntermediateRanking extends RichWindowFunction<Tuple3<Object,Long,Integer>, Tuple1<Rankings>, Object, TimeWindow>{
    private static final Logger LOG = LoggerFactory.getLogger(IntermediateRanking.class);
    Configuration config;
    Metrics metrics = new Metrics();

    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    private final Rankings rankings;

    public IntermediateRanking(Configuration config) {
        this(config, DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public IntermediateRanking(Configuration config, int topN) {
        this(config, topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public IntermediateRanking(Configuration config, int topN, int emitFrequencyInSeconds) {
        if (topN < 1) {
          throw new IllegalArgumentException("topN must be >= 1 (you requested " + topN + ")");
        }
        if (emitFrequencyInSeconds < 1) {
          throw new IllegalArgumentException(
              "The emit frequency must be >= 1 seconds (you requested " + emitFrequencyInSeconds + " seconds)");
        }
        metrics.initialize(config, this.getClass().getSimpleName());
        this.config = config;
        count = topN;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
        rankings = new Rankings(count);
    }

    protected Rankings getRankings() {
        return rankings;
    }

    @Override
    public void apply(Object key, TimeWindow window, Iterable<Tuple3<Object, Long, Integer>> input,
            Collector<Tuple1<Rankings>> out) throws Exception {
        metrics.initialize(config, this.getClass().getSimpleName());
        
        for (Tuple3<Object, Long, Integer> in : input){
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.receiveThroughput();
            }

            Rankable rankable = RankableObjectWithFields.from(in);
            getRankings().updateWith(rankable);

        }

        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.emittedThroughput();
        }
        out.collect(new Tuple1<Rankings>(rankings.copy()));
        LOG.info("Rankings: " + rankings);
    }

    // close method
    @Override
    public void close() throws Exception {
        if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
            metrics.SaveMetrics();
        }
    }

    Logger getLogger() {
        return LOG;
    }
}
