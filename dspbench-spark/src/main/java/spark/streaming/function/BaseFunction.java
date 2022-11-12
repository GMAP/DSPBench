package spark.streaming.function;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.GroupState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.model.gis.Road;
import spark.streaming.util.Configuration;
import spark.streaming.metrics.MetricsFactory;

/**
 * @author mayconbordin
 */
public abstract class BaseFunction implements Serializable {
    private String name;
    private Integer id;
    private transient MetricRegistry metrics;
    private transient Counter tuplesReceived;
    private transient Counter tuplesEmitted;
    private Configuration config;
    private String configStr;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(BaseFunction.class);

    public BaseFunction(Configuration config) {
        this();
        this.configStr = config.toString();
        this.config = config;
        if (config.getBoolean(config.METRICS_ENABLED, false)) {
            File pathTrh = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput").toFile();
            pathTrh.mkdirs();

            this.file = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + ".csv").toFile();
        }
    }

    public BaseFunction(Configuration config, String name) {
        this.name = name;
        this.configStr = config.toString();
    }

    public BaseFunction(String name) {
        this.name = name;
    }

    public BaseFunction() {
        this.name = this.getClass().getSimpleName();
    }

    public void setConfiguration(String cfg) {
        configStr = cfg;
    }

    public Configuration getConfiguration() {
        if (config == null) {
            config = Configuration.fromStr(configStr);
        }

        return config;
    }

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(getConfiguration());
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(String.format("%s-%d.tuples-received", name, getId()));
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(String.format("%s-%d.tuples-emitted", name, getId()));
        }
        return tuplesEmitted;
    }

    protected void incReceived() {
        getTuplesReceived().inc();
    }

    protected void incReceived(long n) {
        getTuplesReceived().inc(n);
    }

    protected void incEmitted() {
        getTuplesEmitted().inc();
    }

    protected void incEmitted(long n) {
        getTuplesEmitted().inc(n);
    }

    protected void incBoth() {
        getTuplesReceived().inc();
        getTuplesEmitted().inc();
    }

    protected int getId() {
        if (id == null) {
            id = Math.abs(UUID.randomUUID().hashCode());
        }
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //public abstract void Calculate()throws InterruptedException;
    public void calculateThroughput() {//delete after

    }

    public Tuple2<Map<String, Long>, BlockingQueue<String>> calculateThroughput(Map<String, Long> throughput, BlockingQueue<String> queue) {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime;
            if (config.get(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }

            Long ops = throughput.get(unixTime + "");
            if (ops == null) {
                for (Map.Entry<String, Long> entry : throughput.entrySet()) {
                    queue.add(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                }
                throughput.clear();
            }

            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
        return new Tuple2<>(throughput, queue);
    }


    public void SaveMetrics(String met) {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(met);
                } catch (IOException ex) {
                    LOG.error("Error while writing the file " + this.file, ex);
                }
            } catch (Exception e) {
                LOG.error("Error while creating the file " + e.getMessage());
            }
        }).start();
    }
}
