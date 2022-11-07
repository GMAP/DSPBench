package flink.util;

import flink.constants.BaseConstants;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Metrics {
    Configuration config;
    private final Map<String, Long> throughput = new HashMap<>();
    private BlockingQueue<String> queue = new ArrayBlockingQueue<>(50);;
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(Metrics.class);

    public void initialize(Configuration config) {
        this.config = config;
        File pathLa = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/gabriel/IDK"), "latency").toFile();
        File pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/gabriel/IDK"), "throughput").toFile();

        pathLa.mkdirs();
        pathTrh.mkdirs();

        this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
    }

    public void initialize(Configuration config, String sinkName) {
        this.config = config;
        if (!this.configPrefix.contains(sinkName)) {
            this.configPrefix = String.format("%s.%s", configPrefix, sinkName);
        }
        File pathLa = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/gabriel/IDK"), "latency").toFile();
        File pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/gabriel/IDK"), "throughput").toFile();

        pathLa.mkdirs();
        pathTrh.mkdirs();

        this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
    }

    public void calculateThroughput() {

        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configurations.METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
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
                if (queue.size() >= 3) {
                    SaveMetrics();
                }
            }

            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
    }

    public void calculateLatency(long UnixTimeInit) {
        if (config.getBoolean(Configurations.METRICS_ENABLED, false)) {
            try {
                FileWriter fw = new FileWriter(Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "latency", this.getClass().getSimpleName() + this.configPrefix + ".csv").toFile(), true);
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
                    System.out.println("Error while writing the file " + file + " - " + ex);
                }
            } catch (Exception e) {
                System.out.println("Error while creating the file " + e.getMessage());
            }
        }).start();
    }
}
