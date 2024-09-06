package org.dspbench.bolt;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.io.FileUtils;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.dspbench.constants.BaseConstants;
import org.dspbench.hooks.BoltMeterHook;
import org.dspbench.metrics.MetricsFactory;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.util.config.Configuration.METRICS_ENABLED;
import static org.dspbench.util.config.Configuration.METRICS_INTERVAL_UNIT;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    protected Map<String, Fields> fields;
    private BlockingQueue<String> queue;
    private File fileReceived;
    private File fileEmitted;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBolt.class);
    private final Map<String, Long> throughput = new HashMap<>();

    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;

    private final Map<String, Long> received = new HashMap<>();
    private final Map<String, Long> emitted = new HashMap<>();

    public AbstractBolt() {
        fields = new HashMap<>();
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseConstants.BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields.isEmpty()) {
            if (getDefaultFields() != null)
                fields.put(BaseConstants.BaseStream.DEFAULT, getDefaultFields());

            if (getDefaultStreamFields() != null)
                fields.putAll(getDefaultStreamFields());
        }

        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    public Fields getDefaultFields() {
        return null;
    }

    public Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(this.config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter(this.getClass().getSimpleName() + "-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter(this.getClass().getSimpleName()+ "-emitted");
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

    public void calculateThroughput() {
        //        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
        //            long unixTime = 0;
        //            if (config.getString(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
        //                unixTime = Instant.now().getEpochSecond();
        //            } else {
        //                unixTime = Instant.now().toEpochMilli();
        //            }
        //
        //            Long ops = throughput.get(unixTime + "");
        //            if (ops == null) {
        //                for (Map.Entry<String, Long> entry : this.throughput.entrySet()) {
        //                    this.queue.add(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
        //                }
        //                throughput.clear();
        //                if (queue.size() >= 10) {
        //                    SaveMetrics();
        //                }
        //            }
        //
        //            ops = (ops == null) ? 1L : ++ops;
        //
        //            throughput.put(unixTime + "", ops);
        //        }
            }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config = Configuration.fromMap(stormConf);
        this.context = context;
        this.collector = collector;
        if (config.getBoolean(METRICS_ENABLED, false)) {
            File pathTrh = Paths.get(config.getString(Configuration.METRICS_OUTPUT)).toFile();

            pathTrh.mkdirs();
            this.fileReceived = Paths.get(config.getString(Configuration.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-received.csv").toFile();
            this.fileEmitted = Paths.get(config.getString(Configuration.METRICS_OUTPUT, "/home/IDK"), this.getClass().getSimpleName() + "-emitted.csv").toFile();
        }
        initialize();
    }

    public void receiveThroughput() {
        if (config.getBoolean(METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long rec = received.get(unixTime + "");

            rec = (rec == null) ? 1L : ++rec;
            received.put(unixTime + "", rec);
        }
    }

    public void emittedThroughput() {
        if (config.getBoolean(METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long emit = emitted.get(unixTime + "");

            emit = (emit == null) ? 1L : ++emit;
            emitted.put(unixTime + "", emit);
        }
    }

    public void recemitThroughput() {
        
        if (config.getBoolean(METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(METRICS_INTERVAL_UNIT, "seconds").equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }
            Long rec = received.get(unixTime + "");
            Long emit = emitted.get(unixTime + "");

            rec = (rec == null) ? 1L : ++rec;
            received.put(unixTime + "", rec);

            emit = (emit == null) ? 1L : ++emit;
            emitted.put(unixTime + "", emit);
        }
    }

    public void SaveMetrics() {
        if (config.getBoolean(METRICS_ENABLED, false)) {
            new Thread(() -> {
                try {
                    try (Writer writer = new FileWriter(this.fileReceived, true)) {
                        for (Map.Entry<String, Long> entry : this.received.entrySet()) {
                            writer.append(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                        }
                        
                    } catch (IOException ex) {
                        System.out.println("Error while writing the file " + fileReceived + " - " + ex);
                    }
                } catch (Exception e) {
                    System.out.println("Error while creating the file " + e.getMessage());
                }
                if (!this.getClass().getSimpleName().contains("Sink")) {
                    try {
                        try (Writer writer = new FileWriter(this.fileEmitted, true)) {
                            for (Map.Entry<String, Long> entry : this.emitted.entrySet()) {
                                writer.append(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                            }
                        } catch (IOException ex) {
                            System.out.println("Error while writing the file " + fileEmitted + " - " + ex);
                        }
                    } catch (Exception e) {
                        System.out.println("Error while creating the file " + e.getMessage());
                    }
                }
            }).start();
        }
    }

    public void initialize() {

    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
}
