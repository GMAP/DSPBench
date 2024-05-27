package flink.application.trendingtopics;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import flink.tools.Rankable;
import flink.tools.RankableObjectWithFields;
import flink.tools.Rankings;
import java.io.*;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import flink.constants.BaseConstants;
import flink.util.Configurations;
import flink.util.MetricsFactory;

public class TotalRankings extends ProcessAllWindowFunction<Tuple1<Rankings>,Tuple1<Rankings>,TimeWindow>{
    private static final Logger LOG = LoggerFactory.getLogger(TotalRankings.class);
    Configuration config;
    
    Metric metrics = new Metric();

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
        metrics.initialize(config);
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
        
        for (Tuple1<Rankings> in : input){
            metrics.incReceived("TotalRankings");
            Rankings rankingsToBeMerged = (Rankings) in.getField(0);
            getRankings().updateWith(rankingsToBeMerged);
            getRankings().pruneZeroCounts();
        }

        //collector.emit(new Values(rankings.copy()));
        out.collect(new Tuple1<Rankings>(rankings.copy()));
        getLogger().debug("Rankings: " + rankings);
    }
}

class Metric implements Serializable {
    Configuration config;
    private final Map<String, Long> throughput = new HashMap<>();
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(150);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(Metric.class);

    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;

    public void initialize(Configuration config) {
        this.config = config;
        getMetrics();
        File pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK")).toFile();

        pathTrh.mkdirs();

        this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
    }

    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    System.out.println("Error while writing the file " + file + " - " + ex);
                }
            } catch (Exception e) {
                System.out.println("Error while creating the file " + e.getMessage());
            }
        }).start();
    }

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived(String name) {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(name + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted(String name) {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(name + "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived(String name) {
        getTuplesReceived(name).inc();
    }

    protected void incReceived(String name, long n) {
        getTuplesReceived(name).inc(n);
    }

    protected void incEmitted(String name) {
        getTuplesEmitted(name).inc();
    }

    protected void incEmitted(String name, long n) {
        getTuplesEmitted(name).inc(n);
    }

    protected void incBoth(String name) {
        getTuplesReceived(name).inc();
        getTuplesEmitted(name).inc();
    }
}