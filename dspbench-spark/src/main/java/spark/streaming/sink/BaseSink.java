package spark.streaming.sink;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import spark.streaming.function.BaseFunction;
import spark.streaming.metrics.MetricsFactory;
import spark.streaming.util.Configuration;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class BaseSink implements Serializable {
    protected Configuration config;
    protected transient SparkSession session;
    private String sinkName;
    private static MetricRegistry metrics;
    private  Counter tuplesReceived;
    private Counter tuplesEmitted;

    public void initialize(Configuration config, SparkSession session) {
        initialize(config, session, "sink");
    }
    public void initialize(Configuration config, SparkSession session, String sinkName) {
        this.config = config;
        this.session = session;
        this.sinkName = sinkName;
        if (config.getBoolean(config.METRICS_ENABLED, false)) {
          /*  File pathLa = Paths.get(config.get(Configuration.METRICS_OUTPUT), "latency").toFile();
            File pathTrh = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput").toFile();

            pathLa.mkdirs();
            pathTrh.mkdirs();
            queue = new ArrayBlockingQueue<>(50);

            this.file = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + ".csv").toFile();*/
        }
    }

//    public Configuration getConfiguration() {
//        if (config == null) {
//            config = Configuration.fromStr(configStr);
//        }
//
//        return config;
//    }

    public abstract DataStreamWriter<Row> sinkStream(Dataset<Row> dt); //Configuration conf

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(this.config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(sinkName + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(sinkName+ "-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived() {
        getTuplesReceived().inc();
    }

    protected void incReceived(Configuration config) {
        this.config = config;
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
    public String getName() {
        return sinkName;
    }


   /* public void calculateLatency(long UnixTimeInit) {
        // new Thread(() -> {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            try {
                FileWriter fw = new FileWriter(Paths.get(config.get(Configuration.METRICS_OUTPUT), "latency", this.getClass().getSimpleName() + this.sinkName + ".csv").toFile(), true);
                fw.write(Instant.now().toEpochMilli() - UnixTimeInit + System.getProperty("line.separator"));
                fw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        //  }).start();
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
    }*/

    public abstract void Calculate(int sink) throws InterruptedException, RuntimeException;
}
