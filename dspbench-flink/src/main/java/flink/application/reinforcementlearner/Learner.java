package flink.application.reinforcementlearner;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.nio.file.Paths;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import flink.application.reinforcementlearner.learner.ReinforcementLearnerFactory;
import flink.application.reinforcementlearner.learner.ReinforcementLearnerInterface;
import flink.constants.BaseConstants;
import flink.constants.ReinforcementLearnerConstants;
import flink.util.Configurations;
import flink.util.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

public class Learner extends RichCoFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, String[]>> {
    private ReinforcementLearnerInterface learner;
    Configuration config;

    Metric metrics = new Metric();

    public Learner(Configuration config){
        metrics.initialize(config);
        this.config = config;

        String learnerType = config.getString(ReinforcementLearnerConstants.Conf.LEARNER_TYPE, "intervalEstimator");
        String[] actions   = config.getString(ReinforcementLearnerConstants.Conf.LEARNER_ACTIONS, "page1,page2,page3").split(",");
        
        learner =  ReinforcementLearnerFactory.create(learnerType, actions, config);
    }

    @Override
    public void flatMap1(Tuple2<String, Integer> value, Collector<Tuple2<String, String[]>> out) throws Exception {
        // EVENT
        String eventID = value.getField(0);
        int roundNum   = value.getField(1);

        String[] actions = learner.nextActions(roundNum);
        metrics.incBoth("Learner");
        out.collect(new Tuple2<String,String[]>(eventID, actions));
    }

    @Override
    public void flatMap2(Tuple2<String, Integer> value, Collector<Tuple2<String, String[]>> out) throws Exception {
        // REWARD
        metrics.incReceived("Learner");
        String action = value.getField(0);
        int reward    = value.getField(1);
        
        learner.setReward(action, reward);
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