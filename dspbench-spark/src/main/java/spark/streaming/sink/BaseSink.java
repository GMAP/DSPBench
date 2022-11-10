package spark.streaming.sink;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.function.BaseFunction;
import spark.streaming.util.Configuration;

import java.io.*;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public abstract class BaseSink implements Serializable {
    protected Configuration config;
    protected transient SparkSession session;
    private final Map<String, Long> throughput = new HashMap<>();

    private BlockingQueue<String> queue;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(BaseFunction.class);

    private String sinkName;

    public void initialize(Configuration config, SparkSession session) {
        initialize(config, session, "sink");
    }

    public void initialize(Configuration config, SparkSession session, String sinkName) {
        this.config = config;
        this.session = session;
        this.sinkName = sinkName;
        if (config.getBoolean(config.METRICS_ENABLED, false)) {
            File pathLa = Paths.get(config.get(Configuration.METRICS_OUTPUT), "latency").toFile();
            File pathTrh = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput").toFile();

            pathLa.mkdirs();
            pathTrh.mkdirs();
            queue = new ArrayBlockingQueue<>(50);

            this.file = Paths.get(config.get(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + ".csv").toFile();
        }
    }

    public abstract DataStreamWriter<Row> sinkStream(Dataset<Row> dt);

    public void calculateThroughput() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.get(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }

            Long ops = throughput.get(unixTime + "");
            if (ops == null) {
                for (Map.Entry<String, Long> entry : this.throughput.entrySet()) {
                    this.queue.add(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                }
                throughput.clear();
                if (queue.size() >= 10) {
                    SaveMetrics();
                }
            }

            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
    }

    public void calculateLatency(long UnixTimeInit) {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            try {
                FileWriter fw = new FileWriter(Paths.get(config.get(Configuration.METRICS_OUTPUT), "latency", this.getClass().getSimpleName() + this.sinkName + ".csv").toFile(), true);
                fw.write(Instant.now().toEpochMilli() - UnixTimeInit + System.getProperty("line.separator"));
                fw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    LOG.error("Error while writing the file " + file, ex);
                }
            } catch (Exception e) {
                LOG.error("Error while creating the file " + e.getMessage());
            }
        }).start();
    }
}
