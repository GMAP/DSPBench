package flink.application.trendingtopics;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import flink.tools.Rankings;
import flink.util.Configurations;
import flink.util.Metrics;

public class TotalRankings extends RichProcessAllWindowFunction<Tuple1<Rankings>,Tuple1<Rankings>,TimeWindow>{
    private static final Logger LOG = LoggerFactory.getLogger(TotalRankings.class);
    Configuration config;
    
    Metrics metrics = new Metrics();

    private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = 2;
    private static final int DEFAULT_COUNT = 10;

    private final int emitFrequencyInSeconds;
    private final int count;
    private final Rankings rankings;

    public TotalRankings(Configuration config) {
        this(config, DEFAULT_COUNT, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public TotalRankings(Configuration config, int topN) {
        this(config, topN, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
    }

    public TotalRankings(Configuration config, int topN, int emitFrequencyInSeconds) {
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

    Logger getLogger() {
        return LOG;
    }

    @Override
    public void process(ProcessAllWindowFunction<Tuple1<Rankings>, Tuple1<Rankings>, TimeWindow>.Context context,
            Iterable<Tuple1<Rankings>> input, Collector<Tuple1<Rankings>> out) throws Exception {
        
        metrics.initialize(config, this.getClass().getSimpleName());
        for (Tuple1<Rankings> in : input){
            if (!config.getBoolean(Configurations.METRICS_ONLY_SINK, false)) {
                metrics.receiveThroughput();
            }
            Rankings rankingsToBeMerged = (Rankings) in.getField(0);
            getRankings().updateWith(rankingsToBeMerged);
            getRankings().pruneZeroCounts();
        }

        //collector.emit(new Values(rankings.copy()));
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
}